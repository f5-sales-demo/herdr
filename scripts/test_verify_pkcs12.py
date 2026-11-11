"""Read real legacy containers and reject wrong credentials without argv disclosure."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / 'scripts/verify-pkcs12.sh'


@unittest.skipUnless(os.name == 'posix' and shutil.which('openssl'), 'PKCS12 reader needs POSIX OpenSSL')
class VerifyPkcs12Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix='herdr-pkcs12-test-')
        cls.root = Path(cls.directory.name)
        cls.password = 'FAKE_PKCS12_PASSWORD_151'
        cls.environment = dict(os.environ, HERDR_TEST_PKCS12_PASSWORD=cls.password)
        help_output = subprocess.run(['openssl', 'pkcs12', '-help'], capture_output=True, text=True)
        options = ['-legacy'] if '-legacy' in help_output.stdout + help_output.stderr else []
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                        '-subj', '/CN=Herdr disposable PKCS12 fixture', '-keyout', str(cls.root / 'test.key'),
                        '-out', str(cls.root / 'test.crt')], capture_output=True, check=True)
        subprocess.run(['openssl', 'pkcs12', '-export', *options, '-inkey', str(cls.root / 'test.key'),
                        '-in', str(cls.root / 'test.crt'), '-out', str(cls.root / 'legacy.p12'),
                        '-passout', 'env:HERDR_TEST_PKCS12_PASSWORD'], env=cls.environment,
                       capture_output=True, check=True)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def invoke(self, name='HERDR_TEST_PKCS12_PASSWORD', **credentials):
        result = subprocess.run(['/bin/sh', str(HELPER), str(self.root / 'legacy.p12'), name],
                                env=dict(self.environment, **credentials), capture_output=True, text=True)
        self.assertNotIn(self.password, result.stdout + result.stderr)
        return result

    def test_reads_actual_legacy_container(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('BEGIN PRIVATE KEY', result.stdout + result.stderr)

    def test_wrong_password_is_rejected(self):
        result = self.invoke(HERDR_TEST_PKCS12_PASSWORD='FAKE_WRONG_PASSWORD')
        self.assertNotEqual(result.returncode, 0)

    def test_invalid_environment_reference_is_rejected(self):
        for name in ['pass:arbitrary', '9INVALID', 'lower-case', '']:
            with self.subTest(name=name):
                self.assertEqual(self.invoke(name).returncode, 64)

    def test_password_is_an_environment_reference_in_openssl_arguments(self):
        with tempfile.TemporaryDirectory(prefix='herdr-pkcs12-argv-') as directory:
            path = Path(directory)
            mock = path / 'openssl'
            mock.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
if '-help' in sys.argv:
    print('-legacy')
else:
    pathlib.Path(os.environ['HERDR_PKCS12_ARGV_RECORD']).write_text(json.dumps(sys.argv[1:]))
''')
            mock.chmod(0o755)
            result = self.invoke(PATH=str(path) + ':' + os.environ['PATH'],
                                 HERDR_PKCS12_ARGV_RECORD=str(path / 'record.json'))
            self.assertEqual(result.returncode, 0, result.stderr)
            argv = json.loads((path / 'record.json').read_text())
            self.assertIn('-legacy', argv)
            self.assertIn('env:HERDR_TEST_PKCS12_PASSWORD', argv)
            self.assertNotIn(self.password, ' '.join(argv))
            self.assertIn('-noout', argv)


if __name__ == '__main__':
    unittest.main()
