#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from __future__ import annotations

import os
from pathlib import Path
from typing import List


class _Certificates:
    """**INTERNAL**"""

    @staticmethod
    def get_certificate_from_file(certpath: str) -> str:
        """
        **INTERNAL** Convenience method for access to the certificate file.  NOT part of the public API.

        Returns:
            str: The contents of the certificate file.
        """
        cert_file = Path(certpath)
        if not cert_file.exists():
            raise FileNotFoundError(f'Certificate file not found: {cert_file}')
        return cert_file.read_text()

    @staticmethod
    def get_capella_certificates() -> List[str]:
        """
        **INTERNAL** Convenience method for access to Capella certificates.  NOT part of the public API.
        Returns:
            List[str]: List of Capella certificates.
        """
        nonprod_cert_dir = Path(Path(__file__).resolve().parent, '_capella_certificates')
        nonprod_certs: List[str] = []
        for cert in nonprod_cert_dir.iterdir():
            if os.path.isdir(cert) or cert.suffix != '.pem':
                continue
            nonprod_certs.append(cert.read_text())
        return nonprod_certs

    @staticmethod
    def get_nonprod_certificates() -> List[str]:
        """
        **INTERNAL** Convenience method for access to non-prod Capella certificates.  NOT
        part of the public API.

        Returns:
            List[str]: List of nonprod Capella certificates.
        """
        import warnings

        warnings.warn('Only use non-prod certificate in DEVELOPMENT environments.', ResourceWarning, stacklevel=2)
        nonprod_cert_dir = Path(Path(__file__).resolve().parent, '_nonprod_certificates')
        nonprod_certs: List[str] = []
        for cert in nonprod_cert_dir.iterdir():
            if os.path.isdir(cert) or cert.suffix != '.pem':
                continue
            nonprod_certs.append(cert.read_text())
        return nonprod_certs
