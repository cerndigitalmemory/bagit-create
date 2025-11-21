"""
This is a proof of concept displaying how warcio can be used to harvest WARC
files of a website of a given URL including its subpages via crawling.

This script uses crawlee for crawling a website starting from START_URL and then
uses warcio to create a single WARC based on the crawled pages.
"""

import asyncio
from io import BytesIO
from urllib.parse import urldefrag, urljoin

from crawlee import Request
from crawlee.crawlers import BeautifulSoupCrawler
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

START_URL = "https://home.cern/"


async def main():
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=500)

    with open("example.warc", "wb") as output:
        writer = WARCWriter(output, gzip=False)

        async def write_warc_record(context):
            url = context.request.url

            payload_bytes = await context.http_response.read()
            payload = BytesIO(payload_bytes)

            status_line = str(context.http_response.status_code)
            http_headers = StatusAndHeaders(
                status_line,
                list(context.http_response.headers.items()),
                protocol=getattr(context.http_response, "http_version", "HTTP/1.1"),
            )

            record = writer.create_warc_record(
                url, "response", payload=payload, http_headers=http_headers
            )
            writer.write_record(record)

        def extract_static_resources(context):
            """Find CSS/JS/images and return absolute URLs."""
            soup = context.soup
            base_url = context.request.url

            urls = set()

            for link in soup.find_all("link", href=True):
                rel = link.get("rel") or []
                if any(
                    r.lower() in ("stylesheet", "preload", "icon", "shortcut icon")
                    for r in rel
                ):
                    urls.add(urljoin(base_url, link["href"]))

            for source in soup.find_all(
                ["script", "img", "video", "audio", "source"], src=True
            ):
                urls.add(urljoin(base_url, source["src"]))

            clean_urls = {urldefrag(url)[0] for url in urls}
            return list(clean_urls)

        @crawler.router.default_handler
        async def request_handler(context):
            await write_warc_record(context)

            content_type = context.http_response.headers.get("content-type", "")
            if "text/html" in content_type:
                await context.enqueue_links()

                asset_urls = extract_static_resources(context)
                if asset_urls:
                    await crawler.add_requests(
                        [Request.from_url(url) for url in asset_urls],
                        forefront=False,
                    )

        await crawler.run([START_URL])


if __name__ == "__main__":
    asyncio.run(main())
