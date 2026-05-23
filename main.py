from fastcore.utils import *
from IPython.display import Markdown
from httpx import get as xget, post as xpost
from fastcore.meta import use_kwargs_dict,delegates
from base64 import b64decode, b64encode
from fasthtml.common import *
from monsterui.all import *
from fasthtml.jupyter import *
from fastlite import *
from pathlib import Path
from datetime import datetime
from shared_ui import *
import os, requests, httpx, asyncio, time, filetype, traceback, hashlib, uuid, mimetypes


# 1. Create/connect to database
db = database('/app/data/apps/receipt_reader/data/receiptapp.db')

# 2. Define table structures
class Receipt: receipt_id: str; business_id: str; uploaded_by_user_id: str | None = None; receipt_name: str; receipt_mime: str; file_hash: str; uploaded_at: str; processing_status: str=""; datalab_request_url: str| None = None; deleted_at: str | None = None;
class Business: business_id: str; business_name: str; created_at: str=""
class User: user_id: str; business_id: str; user_email: str; user_name: str=""; created_at: str=""

# 3. Create tables
bizs = db.create(Business, pk='business_id',not_null={'business_id': True, 'created_at':True},transform=True)
users = db.create(User,pk='user_id',foreign_keys=[('business_id','business','business_id')],not_null={'user_id':True,'business_id':True,'user_email':True, 'created_at':True},transform=True)
receipts = db.create(Receipt, pk='receipt_id',foreign_keys=[('business_id','business','business_id'),('uploaded_by_user_id','user','user_id')],not_null={'receipt_id','business_id','receipt_name','receipt_mime','file_hash','uploaded_at','processing_status'},transform=True)

# 4. Create Index
receipts.create_index(['business_id', 'uploaded_at'], if_not_exists=True) # for finding recent receipts 
receipts.create_index(['business_id', 'file_hash'], unique=True, if_not_exists=True) #  gives you DB-level duplicate enforcement 

# 5. Get table references (for later use)
receipt_table = db.t.receipt
biz_table = db.t.business
user_table = db.t.user


# Utility functions
# "biz", "rcpt" or "user"
def generate_id(prefix: str, n: int = 12) -> str: return f"{prefix}_{uuid.uuid4().hex[:n]}" 
def sha256(p): return hashlib.sha256(p.read_bytes() if isinstance(p, Path) else p).hexdigest()

def save_original_file(paths, data):
    paths.folder.mkdir(parents=True, exist_ok=True)
    Path(paths.original).write_bytes(data)

RECEIPTS_BASE = Path(os.environ.get("RECEIPTS_DATA_DIR", "data/receipts"))
# On pla.sh: Set the environment variable to an absolute path where persistent storage is mounted, like /var/data/receipts

def derive_paths(business_id: str, receipt_id: str, uploaded_at: str, receipt_mime: str):
    y,m,_ = uploaded_at.split("-")
    ext = mimetypes.guess_extension(receipt_mime)
    if ext is None: raise ValueError(f"Unrecognised file type: {receipt_mime}")
    fpath = f"{RECEIPTS_BASE}/{business_id}/{y}/{m}/{receipt_id}{ext}"
    p = Path(fpath)
    mdpath = p.with_suffix(".md")
    fdpath = p.parent
    paths = {"original": fpath,"markdown": mdpath,"folder": fdpath}
    return dict2obj(paths)

# DB Helpers: 
def find_receipt_by_hash(business_id, file_hash): return next(iter(receipts(where="business_id=? AND file_hash=?", where_args=[business_id, file_hash])), None)
def get_receipt(receipt_id): return receipts.get(receipt_id, default=None)
def set_receipt_status(receipt_id, status): receipts.update(dict(receipt_id=receipt_id, processing_status=status))
def insert_receipt(business_id, name, mime, file_hash, uploaded_by_user_id=None): return receipts.insert(dict(receipt_id=generate_id("rcpt"), business_id=business_id, receipt_name=name, receipt_mime=mime, file_hash=file_hash, uploaded_at=datetime.now().isoformat(), processing_status="pending", uploaded_by_user_id=uploaded_by_user_id))


def recent_receipts(business_id, n=10): 
    return receipts(where="business_id=? AND deleted_at IS NULL", where_args=[business_id], order_by="uploaded_at DESC", limit=n)



# Datalab defaults
dlab_params = dict(output_format='markdown', force_ocr=False, format_lines=False, paginate=False, use_llm=False, strip_existing_ocr=False, disable_image_extraction=False, max_pages=None, page_range=None)
dlab_url = "https://www.datalab.to/api/v1/convert"
# "https://www.datalab.to/api/v1/marker" # this endpoint maybe deprecated. https://documentation.datalab.to/api-reference/[deprecated]-marker


@use_kwargs_dict(**dlab_params)
async def submit_marker(fname=None, file=None, file_url=None, key=None, url=dlab_url, timeout=120, retries=3, **kwargs):
    "Submit Images to Datalab Marker API for conversion"
    key = key or os.environ.get("DATALAB_KEY")
    if fname: file = open(fname,"rb")
    try:
        mime = filetype.guess(fname or file).mime
        if not fname: file.seek(0)
        files = {'file': (file.name, file, mime)} if file else None
        if file_url: kwargs['file_url'] = file_url
        async with httpx.AsyncClient(timeout=timeout) as c:
            for i in range(retries):
                try:
                    res = await c.post(url, files=files, data=kwargs, headers={"X-Api-Key": key})
                    data = res.json()
                    if not data.get('success'): raise RuntimeError(f"Submit failed: {data.get('error') or data}")
                    return data
                except (httpx.TimeoutException, httpx.NetworkError) as e:
                    if i == retries-1: raise
                    if not fname: file.seek(0)
                    await asyncio.sleep(2**i)
    finally:
        if fname and file: file.close()

@delegates(submit_marker)
async def submit_markers(files=None, fnames=None, file_urls=None, **kwargs):
    "Submit multiple Images concurrently, return list of response dicts"
    tasks = [submit_marker(file=f, **kwargs) for f in L(files)
        ] + [submit_marker(file_url=u, **kwargs) for u in L(file_urls)
        ] + [submit_marker(fname=u, **kwargs) for u in L(fnames)]
    return await asyncio.gather(*tasks)

async def poll_marker(d, key=None, max_polls=300, delay=2, verbose=False):
    "Poll Marker API until conversion complete"
    if not d.get('success',True): raise ValueError(f"Submit failed: {d.get('error')}")
    check_url = d['request_check_url']
    key = key or os.environ.get("DATALAB_KEY")
    async with httpx.AsyncClient() as c:
        for _ in range(max_polls):
            res = await c.get(check_url, headers={"X-Api-Key": key})
            data = res.json()
            if verbose: print(data["status"], end='; ')
            if data["status"] == "complete": return data
            if data["status"] == "failed": raise RuntimeError(f"Conversion failed: {data.get('error')}")
            await asyncio.sleep(delay)
    raise TimeoutError(f"Polling timed out after {max_polls * delay}s")

async def poll_markers(ds,key=None, max_polls=300, delay=2, verbose=False):
    "Poll multiple Marker API requests concurrently"
    return await asyncio.gather(*[poll_marker(d,key,max_polls,delay,verbose) for d in ds])

@delegates(submit_marker)
async def convert_pdf(fname=None, file=None, file_url=None, key=None, max_polls=300, delay=2, verbose=False, **kwargs):
    "Submit and poll until complete, return result"
    r = await submit_marker(fname=fname,file=file, file_url=file_url, key=key, **kwargs)
    return await poll_marker(r, key=key, max_polls=max_polls, delay=delay, verbose=verbose)

@delegates(submit_markers)
async def convert_pdfs(files=None, fnames=None, file_urls=None, key=None, max_polls=300, delay=2, verbose=False, **kwargs):
    "Submit multiple files and poll all until complete"
    rs = await submit_markers(files=files,fnames=fnames, file_urls=file_urls, key=key, **kwargs)
    return await poll_markers(rs, key=key, max_polls=max_polls, delay=delay, verbose=verbose)

def _save_md(r,stem,path):
    (path/f'{stem}.md').write_text(r['markdown'])
    for nm,dt in r['images'].items(): (path/nm).write_bytes(b64decode(dt))

@delegates(convert_pdf)
async def pdf2md(fname, path='.', **kwargs):
    "Convert PDF to markdown and save with images"
    path = Path(path)
    path.mkdir(exist_ok=True, parents=True)
    r = await convert_pdf(fname=fname, **kwargs)
    _save_md(r, Path(fname).stem, path)
    return r

@delegates(convert_pdfs)
async def pdfs2md(fnames, path='.', **kwargs):
    "Convert multiple PDFs to markdown and save with images"
    path = Path(path)
    path.mkdir(exist_ok=True, parents=True)
    rs = await convert_pdfs(fnames=fnames, **kwargs)
    for fname, r in zip(fnames, rs): _save_md(r,Path(fname).stem, path)
    return rs

copy_js = Script("""
async function copyOut(){const el=document.getElementById('edit');await navigator.clipboard.write([new ClipboardItem({'text/html':new Blob([el.innerHTML],{type:'text/html'}),'text/plain':new Blob([el.innerText],{type:'text/plain'})})])}
function resetOut(){document.getElementById('edit').innerHTML=document.getElementById('orig').innerHTML}
""")

alpine_js = Script(src="https://cdn.jsdelivr.net/npm/alpinejs@3.15.11/dist/cdn.min.js", 
                 defer=True)

app,rt = fast_app(hdrs=(Theme.blue.headers(), copy_js, alpine_js))

def rewrite_image_paths(md, folder):
    folder = Path(folder)
    if folder.is_absolute(): folder = folder.relative_to(RECEIPTS_BASE.parent.parent)
    pattern = r'[a-f0-9]+_img\.(?:jpg|jpeg|png)'
    return re.sub(pattern, rf"/{folder}/\g<0>", md)

def response_ui(mime, data, md): # , img_folder=None
    # if img_folder: md = rewrite_image_paths(md, img_folder)
    src = f"data:{mime};base64,{b64encode(data).decode()}"
    preview = Iframe(src=src, cls='w-full h-96') if 'pdf' in mime else Img(src=src, cls='max-w-full')
    
    outDiv = Div(
        DivLAligned(Button("Copy", onclick="copyOut()", cls=ButtonT.primary),
                    Button("Reset", onclick="resetOut()", cls=ButtonT.secondary), cls='space-x-2 mb-2'),
        Div(render_md(md), id="edit", contenteditable="true", cls='border p-2 rounded'),
        Div(render_md(md), id="orig", cls='hidden'))
    # return out , Div(preview, id="preview", hx_swap_oob="true")
    return Div(outDiv, Div(preview, id="preview", hx_swap_oob="true"), id="receipt-response")

async def process_and_update(receipt_id, paths, mime, data):
    r = await pdf2md(fname=paths.original, path=paths.folder)
    if r["status"] != "complete":
        set_receipt_status(receipt_id, "failed")
        return P("Processing failed. Please try again.", cls="text-red-600")
    set_receipt_status(receipt_id, "done")
    return response_ui(mime, data, r['markdown'])


def receipt_paths(r): return derive_paths(business_id=r.business_id, receipt_id=r.receipt_id, uploaded_at=r.uploaded_at, receipt_mime=r.receipt_mime)

async def render_receipt(r, data=None):
    "Render a receipt's preview + markdown, processing if needed"
    paths = receipt_paths(r)
    if data is None:
        if not Path(paths.original).exists(): return P(f"Original file missing for {r.receipt_name}.", cls="text-red-600")
        data = Path(paths.original).read_bytes()
    elif not Path(paths.original).exists(): save_original_file(paths, data)
    md_path = Path(paths.markdown)
    if md_path.exists(): return response_ui(r.receipt_mime, data, md_path.read_text())
    return await process_and_update(r.receipt_id, paths, r.receipt_mime, data)


def recent_receipts_ui(business_id):
    rs = recent_receipts(business_id)
    return Card(UIGrid(*[A(r.receipt_name,UkIcon('arrow-up-right', height=15, width=15), hx_get=f"/receipt_reselect?receipt_id={r.receipt_id}",hx_target="#output",cls=f"inline-flex items-center gap-1") for r in rs]), 
    header=H3("Recently Added"),cls=f"{SPACE['gap_sm']} wrap")

@rt
async def receipt_reselect(receipt_id: str):
    r = get_receipt(receipt_id)
    if r is None or r.deleted_at: return P("Receipt not found.", cls="text-red-600")
    return await render_receipt(r)

@rt
async def upload(file: UploadFile):
    try:
        data = await file.read()
        mime = filetype.guess(data).mime
        business_id = "biz_seed01"
        file_hash = sha256(data)
        r = find_receipt_by_hash(business_id, file_hash) or insert_receipt(business_id, file.filename, mime, file_hash)
        return await render_receipt(r, data)
    except Exception as e:
        print(traceback.format_exc())
        return Pre(traceback.format_exc(), cls='text-red-600 text-xs whitespace-pre-wrap')

@rt('/home')
def home():
    return PageLayout("PDF/Image → Markdown",
        UISection(UIGrid(
                Card(
                    Div(
                        Input(type="file", accept="image/*,.pdf", **{"@change": "file = $event.target.files[0]"}),
                        UIButton("Convert", **{"@click": "uploadFile()"}, x_bind_disabled="!file || uploading"),
                        Progress(x_show="uploading", **{":value": "progress"}, max=100, cls="mt-2"),
                        P(x_show="uploading", x_text="'Uploading: ' + progress + '%'", cls="text-sm text-gray-500"),
                        Div(id="preview", cls='mt-4'),
                        # below Div show image Immediately.
                        # Div(Img(x_show="file && file.type.startsWith('image/')", **{":src": "file ? URL.createObjectURL(file) : ''"}, cls="max-h-400 mb-4 rounded"), cls='mt-4'),                        
                        x_data="""
                        {
                            file: null,
                            progress: 0,
                            uploading: false,
                            async uploadFile() {
                                if (!this.file) return;
                                this.uploading = true;
                                this.progress = 0;
                                
                                const formData = new FormData();
                                formData.append('file', this.file);
                                
                                const xhr = new XMLHttpRequest();
                                xhr.upload.onprogress = (e) => {
                                    if (e.lengthComputable) {
                                        this.progress = Math.round((e.loaded / e.total) * 100);
                                    }
                                };
                                xhr.onload = () => {
                                        this.uploading = false;
                                        const temp = document.createElement('div');
                                        temp.innerHTML = xhr.responseText;
                                        
                                        const response = temp.querySelector('#receipt-response');
                                        if (response) {
                                            const outDiv = response.children[0];
                                            const previewDiv = response.querySelector('#preview');
                                            
                                            if (outDiv) document.getElementById('output').innerHTML = outDiv.innerHTML;
                                            if (previewDiv) document.getElementById('preview').innerHTML = previewDiv.innerHTML;
                                        }
                                    };                                
                                xhr.open('POST', '/upload');
                                xhr.send(formData);
                            }
                        }
                        """,cls=SPACE['stack_sm']),
                    recent_receipts_ui("biz_seed01"),
                    header=H3("Upload")),                                    
            Card(Div(id="output"), header=H3("Markdown")),            
            cols='grid_2',align='start')),
            nav=SiteNav(brand=BRAND,user='Naveen'),
            footer= SiteFooter(brand=BRAND,cls="bg-gray-200")
    )