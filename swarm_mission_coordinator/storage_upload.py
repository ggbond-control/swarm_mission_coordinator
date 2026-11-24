"""Conditional S3 report writes, independent of rendering and job scheduling."""

import hashlib
import json


def conditional_upload(storage, key, content, epoch, version, *, authorized=lambda: True):
    """CAS prevents a delayed older leader from overwriting newer report bytes.

    Fail explicitly if the installed SDK/server does not implement conditional
    PUT. Never fall back to an unconditional overwrite.
    """
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    credentials = storage["stsCredentials"]
    client = boto3.client(
        "s3",
        endpoint_url=storage["endpoint"],
        aws_access_key_id=credentials["accessKey"],
        aws_secret_access_key=credentials["secretKey"],
        aws_session_token=credentials.get("sessionToken") or None,
        config=Config(
            connect_timeout=3,
            read_timeout=10,
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )
    try:
        condition = {"IfNoneMatch": "*"}
        try:
            old = client.head_object(Bucket=storage["bucket"], Key=key)
            metadata = old.get("Metadata", {})
            if "swarm-epoch" not in metadata or "swarm-version" not in metadata:
                raise RuntimeError("refusing to overwrite unversioned report")
            previous = int(metadata["swarm-epoch"]), int(metadata["swarm-version"])
            if previous > (epoch, version):
                return "superseded"
            if (
                previous == (epoch, version)
                and metadata.get("sha256") == hashlib.sha256(content).hexdigest()
            ):
                return "unchanged"
            if previous == (epoch, version):
                # Same terminal version: only add evidence. A stale partial
                # replica cannot replace a report that already has more images.
                prior = client.get_object(Bucket=storage["bucket"], Key=key)["Body"]
                try:
                    prior_content = prior.read()
                finally:
                    prior.close()
                marker = b'<script type="application/json" id="swarm-media-manifest">'

                def media_ids(raw):
                    return (
                        set(
                            json.loads(
                                raw.split(marker, 1)[1].split(b"</script>", 1)[0]
                            )
                        )
                        if marker in raw
                        else set()
                    )

                if not media_ids(prior_content).issubset(media_ids(content)):
                    return "superseded"
            condition = {"IfMatch": old["ETag"]}
        except ClientError as error:
            # A prefix-scoped credential can see HTTP 403 for a missing key
            # when ListBucket is denied. Conditional create still cannot
            # replace an existing object.
            if error.response["Error"]["Code"] not in {"403", "404", "AccessDenied", "NoSuchKey", "NotFound"}:
                raise
        # Ubuntu Jazzy's botocore may lack conditional PUT model fields. Sign
        # the actual HTTP headers with SigV4 rather than dropping the condition.
        import http.client
        from urllib.parse import quote, urlsplit

        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        endpoint = urlsplit(storage["endpoint"])
        path = "/" + quote(storage["bucket"], safe="") + "/" + quote(key, safe="/")
        headers = {
            "Content-Type": "text/html; charset=utf-8",
            "Content-Length": str(len(content)),
            "x-amz-content-sha256": hashlib.sha256(content).hexdigest(),
            "x-amz-meta-swarm-epoch": str(epoch),
            "x-amz-meta-swarm-version": str(version),
            "x-amz-meta-sha256": hashlib.sha256(content).hexdigest(),
            **(
                {"If-Match": condition["IfMatch"]}
                if "IfMatch" in condition
                else {"If-None-Match": "*"}
            ),
        }
        request = AWSRequest(
            method="PUT",
            url=storage["endpoint"].rstrip("/") + path,
            data=content,
            headers=headers,
        )
        SigV4Auth(
            Credentials(
                credentials["accessKey"],
                credentials["secretKey"],
                credentials.get("sessionToken") or None,
            ),
            "s3",
            "us-east-1",
        ).add_auth(request)
        connection_class = (
            http.client.HTTPSConnection
            if endpoint.scheme == "https"
            else http.client.HTTPConnection
        )
        connection = connection_class(endpoint.hostname, endpoint.port, timeout=3)
        try:
            connection.connect()
            if not authorized():
                return "deferred"
            connection.request("PUT", path, body=content, headers=dict(request.headers))
            if connection.sock:
                connection.sock.settimeout(10)
            response = connection.getresponse()
            response.read()
            if response.status not in {200, 201, 204}:
                raise RuntimeError(f"conditional S3 PUT failed: HTTP {response.status}")
        finally:
            connection.close()
        return "uploaded"
    finally:
        client.close()
