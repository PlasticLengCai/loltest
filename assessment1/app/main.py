import os
import sqlite3
from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from starlette.requests import Request
from datetime import datetime, timedelta
from typing import Optional
import jwt
import uuid
import shutil
import subprocess
from PIL import Image
import requests

app = FastAPI()
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_DIR = os.path.join(BASE_DIR, "database")
VIDEO_ORIG_DIR = os.path.join(DB_DIR, "videos", "original")
VIDEO_TRAN_DIR = os.path.join(DB_DIR, "videos", "transcoded")
IMG_ORIG_DIR = os.path.join(DB_DIR, "images", "original")
IMG_THUM_DIR = os.path.join(DB_DIR, "images", "thumbs")
DB_PATH = os.path.join(DB_DIR, "app.db")
SECRET_KEY = os.environ.get("SECRET_KEY", "change-me")
ALGORITHM = "HS256"
TOKEN_MIN = 60

def ensure_dirs():
    for p in [DB_DIR, VIDEO_ORIG_DIR, VIDEO_TRAN_DIR, IMG_ORIG_DIR, IMG_THUM_DIR]:
        os.makedirs(p, exist_ok=True)

def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT UNIQUE, password TEXT, role TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS videos (id INTEGER PRIMARY KEY, owner TEXT, orig_filename TEXT, stored_filename TEXT, status TEXT, output_filename TEXT, uploaded_at TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS images (id INTEGER PRIMARY KEY, owner TEXT, orig_filename TEXT, stored_filename TEXT, thumb_filename TEXT, uploaded_at TEXT)")
    cur.execute("INSERT OR IGNORE INTO users(username,password,role) VALUES(?,?,?)", ("admin","admin","admin"))
    cur.execute("INSERT OR IGNORE INTO users(username,password,role) VALUES(?,?,?)", ("user2","pass2","user"))
    conn.commit()
    conn.close()

def create_token(sub: str, role: str):
    exp = datetime.utcnow() + timedelta(minutes=TOKEN_MIN)
    return jwt.encode({"sub": sub, "role": role, "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)

def get_identity(request: Request):
    auth = request.headers.get("authorization") or ""
    token = None
    if auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1]
    if not token:
        token = request.query_params.get("token")
    if not token:
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        sub = payload.get("sub")
        role = payload.get("role")
        if not sub:
            raise HTTPException(status_code=401, detail="unauthorized")
        return sub, role
    except Exception:
        raise HTTPException(status_code=401, detail="unauthorized")

def owner_or_admin(user: str, role: str, owner: str):
    if role == "admin":
        return True
    return user == owner

@app.on_event("startup")
async def startup():
    ensure_dirs()
    init_db()

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT username,password,role FROM users WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=401, detail="unauthorized")
    if password != row["password"]:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = create_token(row["username"], row["role"])
    return {"access_token": token, "token_type": "bearer"}

@app.get("/whoami")
def whoami(identity: tuple = Depends(get_identity)):
    user, role = identity
    return {"user": user, "role": role}

@app.post("/upload/video")
def upload_video(file: UploadFile = File(...), identity: tuple = Depends(get_identity)):
    user, role = identity
    filename = file.filename or "video"
    uid = str(uuid.uuid4())
    stored = uid + "_" + filename
    dest = os.path.join(VIDEO_ORIG_DIR, stored)
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    conn = db()
    cur = conn.cursor()
    cur.execute("INSERT INTO videos(owner,orig_filename,stored_filename,status,output_filename,uploaded_at) VALUES(?,?,?,?,?,?)",
                (user, filename, stored, "uploaded", None, datetime.utcnow().isoformat()))
    vid = cur.lastrowid
    conn.commit()
    conn.close()
    return {"id": vid, "owner": user, "status": "uploaded"}

@app.post("/transcode/{video_id}")
def transcode_video(video_id: int, identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id,owner,stored_filename,status,output_filename FROM videos WHERE id=?", (video_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="not found")
    if not owner_or_admin(user, role, row["owner"]):
        conn.close()
        raise HTTPException(status_code=403, detail="forbidden")
    in_path = os.path.join(VIDEO_ORIG_DIR, row["stored_filename"])
    base = os.path.splitext(row["stored_filename"])[0]
    out_name = base + "_720p.mp4"
    out_path = os.path.join(VIDEO_TRAN_DIR, out_name)
    if not os.path.exists(in_path):
        conn.close()
        raise HTTPException(status_code=404, detail="missing source")
    if row["status"] == "processing":
        conn.close()
        return {"id": row["id"], "status": "processing"}
    cur.execute("UPDATE videos SET status=?, output_filename=? WHERE id=?", ("processing", out_name, video_id))
    conn.commit()
    try:
        cmd = ["ffmpeg","-y","-i",in_path,"-c:v","libx264","-preset","veryfast","-crf","28","-vf","scale=-2:720","-c:a","aac","-b:a","128k",out_path]
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cur.execute("UPDATE videos SET status=? WHERE id=?", ("completed", video_id))
        conn.commit()
        conn.close()
        return {"id": video_id, "status": "completed", "output": out_name}
    except Exception:
        cur.execute("UPDATE videos SET status=? WHERE id=?", ("failed", video_id))
        conn.commit()
        conn.close()
        raise HTTPException(status_code=500, detail="transcode failed")

@app.get("/videos")
def list_videos(limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0), status: Optional[str] = None, identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    if role == "admin":
        if status:
            cur.execute("SELECT id,owner,orig_filename,status,output_filename,uploaded_at FROM videos WHERE status=? ORDER BY id DESC LIMIT ? OFFSET ?", (status, limit, offset))
        else:
            cur.execute("SELECT id,owner,orig_filename,status,output_filename,uploaded_at FROM videos ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))
    else:
        if status:
            cur.execute("SELECT id,owner,orig_filename,status,output_filename,uploaded_at FROM videos WHERE owner=? AND status=? ORDER BY id DESC LIMIT ? OFFSET ?", (user, status, limit, offset))
        else:
            cur.execute("SELECT id,owner,orig_filename,status,output_filename,uploaded_at FROM videos WHERE owner=? ORDER BY id DESC LIMIT ? OFFSET ?", (user, limit, offset))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"items": rows, "limit": limit, "offset": offset}

@app.get("/videos/{video_id}")
def get_video(video_id: int, identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id,owner,orig_filename,stored_filename,status,output_filename,uploaded_at FROM videos WHERE id=?", (video_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    if not owner_or_admin(user, role, row["owner"]):
        raise HTTPException(status_code=403, detail="forbidden")
    return dict(row)

@app.get("/videos/{video_id}/download")
def download_video(video_id: int, identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT owner,stored_filename,orig_filename FROM videos WHERE id=?", (video_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    if not owner_or_admin(user, role, row["owner"]):
        raise HTTPException(status_code=403, detail="forbidden")
    path = os.path.join(VIDEO_ORIG_DIR, row["stored_filename"])
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="missing")
    return FileResponse(path, filename=row["orig_filename"], media_type="application/octet-stream")

@app.get("/videos/{video_id}/download_transcoded")
def download_video_transcoded(video_id: int, identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT owner,output_filename FROM videos WHERE id=?", (video_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    if not owner_or_admin(user, role, row["owner"]):
        raise HTTPException(status_code=403, detail="forbidden")
    if not row["output_filename"]:
        raise HTTPException(status_code=409, detail="not ready")
    path = os.path.join(VIDEO_TRAN_DIR, row["output_filename"])
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="missing")
    return FileResponse(path, filename=row["output_filename"], media_type="application/octet-stream")

@app.post("/upload/image")
def upload_image(file: UploadFile = File(...), identity: tuple = Depends(get_identity)):
    user, role = identity
    filename = file.filename or "image"
    uid = str(uuid.uuid4())
    stored = uid + "_" + filename
    dest = os.path.join(IMG_ORIG_DIR, stored)
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    thumb_name = uid + "_thumb.jpg"
    thumb_path = os.path.join(IMG_THUM_DIR, thumb_name)
    try:
        img = Image.open(dest)
        img.thumbnail((320, 320))
        img = img.convert("RGB")
        img.save(thumb_path, "JPEG")
    except Exception:
        thumb_name = None
    conn = db()
    cur = conn.cursor()
    cur.execute("INSERT INTO images(owner,orig_filename,stored_filename,thumb_filename,uploaded_at) VALUES(?,?,?,?,?)",
                (user, filename, stored, thumb_name, datetime.utcnow().isoformat()))
    iid = cur.lastrowid
    conn.commit()
    conn.close()
    return {"id": iid, "owner": user}

@app.get("/images")
def list_images(limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0), identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    if role == "admin":
        cur.execute("SELECT id,owner,orig_filename,stored_filename,thumb_filename,uploaded_at FROM images ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))
    else:
        cur.execute("SELECT id,owner,orig_filename,stored_filename,thumb_filename,uploaded_at FROM images WHERE owner=? ORDER BY id DESC LIMIT ? OFFSET ?", (user, limit, offset))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"items": rows, "limit": limit, "offset": offset}

@app.get("/images/{image_id}/download")
def download_image(image_id: int, identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT owner,stored_filename,orig_filename FROM images WHERE id=?", (image_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    if not owner_or_admin(user, role, row["owner"]):
        raise HTTPException(status_code=403, detail="forbidden")
    path = os.path.join(IMG_ORIG_DIR, row["stored_filename"])
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="missing")
    return FileResponse(path, filename=row["orig_filename"], media_type="application/octet-stream")

@app.get("/images/{image_id}/thumbnail")
def download_image_thumb(image_id: int, identity: tuple = Depends(get_identity)):
    user, role = identity
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT owner,thumb_filename FROM images WHERE id=?", (image_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    if not owner_or_admin(user, role, row["owner"]):
        raise HTTPException(status_code=403, detail="forbidden")
    if not row["thumb_filename"]:
        raise HTTPException(status_code=404, detail="missing")
    path = os.path.join(IMG_THUM_DIR, row["thumb_filename"])
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="missing")
    return FileResponse(path, filename=row["thumb_filename"], media_type="image/jpeg")

@app.get("/ui")
def ui():
    p = os.path.join(BASE_DIR, "index.html")
    with open(p, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

