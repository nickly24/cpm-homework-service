import tempfile
import unittest
from pathlib import Path

import pikepdf

from homework_service.pdf_pipeline import PdfRejected, process_pdf


class PdfPipelineTests(unittest.TestCase):
    def make_pdf(self, path, pages=1, password=None):
        pdf = pikepdf.Pdf.new()
        for _ in range(pages):
            pdf.add_blank_page(page_size=(595, 842))
        if password:
            pdf.save(path, encryption=pikepdf.Encryption(owner=password, user=password))
        else:
            pdf.save(path)

    def test_valid_pdf_and_page_limit(self):
        with tempfile.TemporaryDirectory() as folder:
            source, output = Path(folder) / 'in.pdf', Path(folder) / 'out.pdf'
            self.make_pdf(source, pages=35)
            result = process_pdf(source, output, 10 * 1024 * 1024, 35)
            self.assertEqual(result['page_count'], 35)
            self.assertEqual(len(result['sha256']), 64)

    def test_36_pages_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            source, output = Path(folder) / 'in.pdf', Path(folder) / 'out.pdf'
            self.make_pdf(source, pages=36)
            with self.assertRaisesRegex(PdfRejected, 'too_many_pages'):
                process_pdf(source, output, 10 * 1024 * 1024, 35)

    def test_password_pdf_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            source, output = Path(folder) / 'in.pdf', Path(folder) / 'out.pdf'
            self.make_pdf(source, password='secret')
            with self.assertRaises(PdfRejected) as raised:
                process_pdf(source, output, 10 * 1024 * 1024, 35)
            self.assertIn(raised.exception.code, {'encrypted_pdf', 'corrupt_pdf'})

    def test_active_javascript_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            source, output = Path(folder) / 'in.pdf', Path(folder) / 'out.pdf'
            pdf = pikepdf.Pdf.new()
            pdf.add_blank_page(page_size=(595, 842))
            pdf.Root.OpenAction = pdf.make_indirect(
                pikepdf.Dictionary(S=pikepdf.Name('/JavaScript'), JS='app.alert(1)')
            )
            pdf.save(source)
            with self.assertRaisesRegex(PdfRejected, 'active_content'):
                process_pdf(source, output, 10 * 1024 * 1024, 35)

    def test_bad_signature_and_oversize_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            source, output = Path(folder) / 'in.pdf', Path(folder) / 'out.pdf'
            source.write_bytes(b'not-a-pdf')
            with self.assertRaisesRegex(PdfRejected, 'invalid_pdf_signature'):
                process_pdf(source, output, 100, 35)
            source.write_bytes(b'%PDF-' + b'x' * 96)
            with self.assertRaisesRegex(PdfRejected, 'source_too_large'):
                process_pdf(source, output, 100, 35)


if __name__ == '__main__':
    unittest.main()
