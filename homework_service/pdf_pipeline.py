import hashlib
from pathlib import Path


class PdfRejected(ValueError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _has_active_content(pdf):
    root = pdf.Root
    if any(key in root for key in ('/OpenAction', '/AA')):
        return True
    names = root.get('/Names')
    if names and any(key in names for key in ('/JavaScript', '/EmbeddedFiles')):
        return True
    acro = root.get('/AcroForm')
    if acro and '/XFA' in acro:
        return True
    dangerous_keys = {
        '/JS', '/JavaScript', '/Launch', '/RichMedia', '/EmbeddedFile',
        '/AA', '/OpenAction', '/SubmitForm', '/ImportData', '/Movie',
        '/Sound', '/3D', '/Rendition',
    }
    dangerous_actions = {
        '/JavaScript', '/Launch', '/GoToR', '/SubmitForm', '/ImportData',
        '/Movie', '/Sound', '/Rendition',
    }
    for obj in pdf.objects:
        try:
            if any(key in obj for key in dangerous_keys):
                return True
            if obj.get('/S') in dangerous_actions:
                return True
        except (AttributeError, TypeError):
            continue
    return False


def process_pdf(source_path, output_path, max_bytes, max_pages):
    source_path, output_path = Path(source_path), Path(output_path)
    if source_path.stat().st_size > max_bytes:
        raise PdfRejected('source_too_large')
    with source_path.open('rb') as stream:
        if stream.read(5) != b'%PDF-':
            raise PdfRejected('invalid_pdf_signature')
    try:
        import pikepdf
        with pikepdf.open(source_path, password='') as pdf:
            if pdf.is_encrypted:
                raise PdfRejected('encrypted_pdf')
            page_count = len(pdf.pages)
            if page_count < 1:
                raise PdfRejected('empty_pdf')
            if page_count > max_pages:
                raise PdfRejected('too_many_pages')
            if _has_active_content(pdf):
                raise PdfRejected('active_content')
            with pdf.open_metadata(set_pikepdf_as_editor=False, update_docinfo=False) as meta:
                meta.clear()
            pdf.save(
                output_path,
                linearize=True,
                compress_streams=True,
                object_stream_mode=pikepdf.ObjectStreamMode.generate,
            )
    except PdfRejected:
        raise
    except Exception as exc:
        if 'password' in type(exc).__name__.lower():
            raise PdfRejected('encrypted_pdf') from exc
        raise PdfRejected('corrupt_pdf') from exc
    size = output_path.stat().st_size
    if size > max_bytes:
        output_path.unlink(missing_ok=True)
        raise PdfRejected('processed_too_large')
    digest = hashlib.sha256(output_path.read_bytes()).hexdigest()
    return {'size_bytes': size, 'page_count': page_count, 'sha256': digest}
