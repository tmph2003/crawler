import asyncio
import logging
import dataclasses
from typing import Any
import random
import json

from playwright.async_api import async_playwright, TimeoutError
from urllib.parse import urlencode, quote, urlparse
from common.stealth import stealth_async

from .playwright_exceptions import (
    InvalidJSONException,
    EmptyResponseException,
)

def random_choice(choices: list):
    """Return a random choice from a list, or None if the list is empty"""
    if choices is None or len(choices) == 0:
        return None
    return random.choice(choices)

@dataclasses.dataclass
class PlaywrightSession:
    """A session using Playwright"""

    context: Any
    page: Any
    proxy: str = None
    params: dict = None
    headers: dict = None
    ms_token: str = None
    base_url: str = None # example: "https://www.tiktok.com"

class PlaywrightHelper:
    """The main Playwright_Helper class that contains all the endpoints.

    Import With:
        .. code-block:: python

            from common.playwright_helper import playwright_helper
            api = PlaywrightHelper()
    """

    def __init__(self, logging_level: int = logging.WARN, logger_name: str = None):
        """
        Create a PlaywrightHelper object.

        Args:
            logging_level (int): The logging level you want to use.
            logger_name (str): The name of the logger you want to use.
        """
        self.sessions = []

        if logger_name is None:
            logger_name = __name__
        self.__create_logger(logger_name, logging_level)

    def __create_logger(self, name: str, level: int = logging.DEBUG):
        """Create a logger for the class."""
        self.logger: logging.Logger = logging.getLogger(name)
        self.logger.setLevel(level)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    async def __set_session_params(self, session: PlaywrightSession, params_additional: dict = None):
        """Set the session params for a PlaywrightSession"""
        user_agent = await session.page.evaluate("() => navigator.userAgent")
        language = await session.page.evaluate(
            "() => navigator.language || navigator.userLanguage"
        )
        platform = await session.page.evaluate("() => navigator.platform")
        device_id = str(random.randint(10**18, 10**19 - 1))  # Random device id
        history_len = str(random.randint(1, 10))  # Random history length
        screen_height = str(random.randint(600, 1080))  # Random screen height
        screen_width = str(random.randint(800, 1920))  # Random screen width
        timezone = await session.page.evaluate(
            "() => Intl.DateTimeFormat().resolvedOptions().timeZone"
        )

        session_params = {
            "app_language": language,
            "browser_language": language,
            "browser_platform": platform,
            "browser_version": user_agent,
            "cookie_enabled": "true",
            "device_id": device_id,
            "history_len": history_len,
            "language": language,
            "os": platform,
            "screen_height": screen_height,
            "screen_width": screen_width,
            "tz_name": timezone,
            "webcast_language": language,
        }
        if params_additional:
            session_params = {**session_params, **params_additional}
        session.params = session_params

    async def __create_session(
        self,
        url: str = None,
        proxy: str = None,
        context_options: dict = {},
        cookies: dict = None,
        suppress_resource_load_types: list[str] = None,
        timeout: int = 30000,
    ):
        try:
            """Create a PlaywrightSession"""
            context = await self.browser.new_context(proxy=proxy, **context_options)
            if cookies is not None:
                formatted_cookies = [
                    {"name": k, "value": v, "domain": urlparse(url).netloc, "path": "/"}
                    for k, v in cookies.items()
                    if v is not None
                ]
                await context.add_cookies(formatted_cookies)
            page = await context.new_page()
            await stealth_async(page)
    
            # Get the request headers to the url
            request_headers = None
    
            def handle_request(request):
                nonlocal request_headers
                request_headers = request.headers
    
            page.once("request", handle_request)
    
            if suppress_resource_load_types is not None:
                await page.route(
                    "**/*",
                    lambda route, request: route.abort()
                    if request.resource_type in suppress_resource_load_types
                    else route.continue_(),
                )
            
            # Set the navigation timeout
            page.set_default_navigation_timeout(timeout)
    
            await page.goto(url)
            await page.goto(url) # hack: many website blocks first request not sure why, likely bot detection
            
            # by doing this, we are simulate scroll event using mouse to `avoid` bot detection
            x, y = random.randint(0, 50), random.randint(0, 50)
            a, b = random.randint(1, 50), random.randint(100, 200)
    
            await page.mouse.move(x, y)
            # await page.wait_for_load_state("networkidle")
            await page.mouse.move(a, b)
    
            session = PlaywrightSession(
                context,
                page,
                proxy=proxy,
                headers=request_headers,
                base_url=url,
            )
            self.sessions.append(session)
            await self.__set_session_params(session)
        except Exception as e:
            # clean up
            self.logger.error(f"Failed to create session: {e}")
            # Cleanup resources if they were partially created
            if 'page' in locals():
                await page.close()
            if 'context' in locals():
                await context.close()
            raise  # Re-raise the exception after cleanup

    async def create_sessions(
        self,
        num_sessions=5,
        headless=True,
        proxies: list = None,
        starting_url: str = None,
        context_options: dict = {},
        override_browser_args: list[dict] = None,
        cookies: list[dict] = None,
        suppress_resource_load_types: list[str] = None,
        browser: str = "chromium",
        executable_path: str = None,
        timeout: int = 30000,
    ):
        """
        Create sessions for use within the PlaywrightHelper class.

        These sessions are what will carry out requesting your data from website.

        Args:
            num_sessions (int): The amount of sessions you want to create.
            headless (bool): Whether or not you want the browser to be headless.
            proxies (list): A list of proxies to use for the sessions
            sleep_after (int): The amount of time to sleep after creating a session, this is to allow the msToken to be generated.
            starting_url (str): The url to start the sessions on.
            context_options (dict): Options to pass to the playwright context.
            override_browser_args (list[dict]): A list of dictionaries containing arguments to pass to the browser.
            cookies (list[dict]): A list of cookies to use for the sessions, you can get these from your cookies after visiting website.
            suppress_resource_load_types (list[str]): Types of resources to suppress playwright from loading, excluding more types will make playwright faster.. Types: document, stylesheet, image, media, font, script, textrack, xhr, fetch, eventsource, websocket, manifest, other.
            browser (str): firefox, chromium, or webkit; default is chromium
            executable_path (str): Path to the browser executable
            timeout (int): The timeout in milliseconds for page navigation
        """
        self.playwright = await async_playwright().start()
        if browser == "chromium":
            if headless and override_browser_args is None:
                override_browser_args = ["--headless=new"]
                headless = False  # managed by the arg
            self.browser = await self.playwright.chromium.launch(
                headless=headless, args=override_browser_args, proxy=random_choice(proxies), executable_path=executable_path
            )
        elif browser == "firefox":
            self.browser = await self.playwright.firefox.launch(
                headless=headless, args=override_browser_args, proxy=random_choice(proxies), executable_path=executable_path
            )
        elif browser == "webkit":
            self.browser = await self.playwright.webkit.launch(
                headless=headless, args=override_browser_args, proxy=random_choice(proxies), executable_path=executable_path
            )
        else:
            raise ValueError("Invalid browser argument passed")
        await asyncio.gather(
            *(
                self.__create_session(
                    proxy=random_choice(proxies),
                    url=starting_url,
                    context_options=context_options,
                    cookies=random_choice(cookies),
                    suppress_resource_load_types=suppress_resource_load_types,
                    timeout=timeout,
                )
                for _ in range(num_sessions)
            )
        )

    async def close_sessions(self):
        """
        Close all the sessions. Should be called when you're done with the PlaywrightHelper object

        This is called automatically when using the PlaywrightHelper with "with"
        """
        for session in self.sessions:
            await session.page.close()
            await session.context.close()
        self.sessions.clear()

        await self.browser.close()
        await self.playwright.stop()

    def generate_js_fetch(self, method: str, url: str, headers: dict) -> str:
        """Generate a javascript fetch function for use in playwright"""
        headers_js = json.dumps(headers)
        print(f"""
            () => {{
                return new Promise((resolve, reject) => {{
                    fetch('{url}', {{ method: '{method}', headers: {headers_js} }})
                        .then(response => response.text())
                        .then(data => resolve(data))
                        .catch(error => reject(error.message));
                }});
            }}
        """)
        return f"""
            () => {{
                return new Promise((resolve, reject) => {{
                    fetch('{url}', {{ method: '{method}', headers: {headers_js} }})
                        .then(response => response.text())
                        .then(data => resolve(data))
                        .catch(error => reject(error.message));
                }});
            }}
        """

    def _get_session(self, **kwargs):
        """Get a random session

        Args:
            session_index (int): The index of the session you want to use, if not provided a random session will be used.

        Returns:
            int: The index of the session.
            PlaywrightSession: The session.
        """
        if len(self.sessions) == 0:
            raise Exception("No sessions created, please create sessions first")

        if kwargs.get("session_index") is not None:
            i = kwargs["session_index"]
        else:
            i = random.randint(0, len(self.sessions) - 1)
        return i, self.sessions[i]

    async def set_session_cookies(self, session, cookies):
        """
        Set the cookies for a session

        Args:
            session (PlaywrightSession): The session to set the cookies for.
            cookies (dict): The cookies to set for the session.
        """
        await session.context.add_cookies(cookies)

    async def get_session_cookies(self, session):
        """
        Get the cookies for a session

        Args:
            session (PlaywrightSession): The session to get the cookies for.

        Returns:
            dict: The cookies for the session.
        """
        cookies = await session.context.cookies()
        return {cookie["name"]: cookie["value"] for cookie in cookies}

    async def run_fetch_script(self, url: str, headers: dict, **kwargs):
        """
        Fetch một URL bằng Playwright context.request (tránh CORS).

        Args:
            url (str): Đường dẫn cần fetch.
            headers (dict): Headers gửi kèm request.

        Returns:
            str: Response body (text).
        """
        _, session = self._get_session(**kwargs)

        response = await session.context.request.get(url, headers=headers)

        if not response.ok:
            # ném lỗi rõ ràng để dễ debug
            raise Exception(f"Request failed {response.status}: {await response.text()}")

        return await response.text()


    async def generate_x_bogus(self, url: str, **kwargs):
        """Generate the X-Bogus header for a url"""
        _, session = self._get_session(**kwargs)

        max_attempts = 5
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            try:
                timeout_time = random.randint(5000, 20000)
                await session.page.wait_for_function("window.byted_acrawler !== undefined", timeout=timeout_time)
                break
            except TimeoutError as e:
                if attempts == max_attempts:
                    raise TimeoutError(f"Failed to load tiktok after {max_attempts} attempts, consider using a proxy")
                
                try_urls = ["https://www.tiktok.com/foryou", "https://www.tiktok.com", "https://www.tiktok.com/@tiktok", "https://www.tiktok.com/foryou"]

                await session.page.goto(random_choice(try_urls))
        
        result = await session.page.evaluate(
            f'() => {{ return window.byted_acrawler.frontierSign("{url}") }}'
        )
        return result

    async def sign_url(self, url: str, **kwargs):
        """Sign a url"""
        i, session = self._get_session(**kwargs)

        # TODO: Would be nice to generate msToken here

        # Add X-Bogus to url
        x_bogus = (await self.generate_x_bogus(url, session_index=i)).get("X-Bogus")
        if x_bogus is None:
            raise Exception("Failed to generate X-Bogus")

        if "?" in url:
            url += "&"
        else:
            url += "?"
        url += f"X-Bogus={x_bogus}"

        return url

    async def make_request(
        self,
        url: str,
        headers: dict = None,
        params: dict = None,
        retries: int = 3,
        exponential_backoff: bool = True,
        is_sign_url: bool = False,
        **kwargs,
    ):
        """
        Makes a request to website through a session.

        Args:
            url (str): The url to make the request to.
            headers (dict): The headers to use for the request.
            params (dict): The params to use for the request.
            retries (int): The amount of times to retry the request if it fails.
            exponential_backoff (bool): Whether or not to use exponential backoff when retrying the request.
            session_index (int): The index of the session you want to use, if not provided a random session will be used.

        Returns:
            dict: The json response from website.

        Raises:
            Exception: If the request fails.
        """
        i, session = self._get_session(**kwargs)
        if session.params is not None:
            params = {**(session.params or {}), **(params or {})}
        else:
            params = session.params

        if headers is not None:
            headers = {**(session.headers or {}), **(headers or {})}
        else:
            headers = session.headers

        if is_sign_url:
            encoded_params = f"{url}?{urlencode(params, safe='=', quote_via=quote)}"
            final_url = await self.sign_url(encoded_params, session_index=i)
        else:
            final_url = f"{url}?{urlencode(params, safe='=', quote_via=quote)}"

        retry_count = 0
        while retry_count < retries:
            retry_count += 1
            result = await self.run_fetch_script(
                final_url, headers=headers, session_index=i
            )

            if result is None:
                raise Exception("PlaywrightHelper.run_fetch_script returned None")

            if result == "":
                raise EmptyResponseException(result, "Website returned an empty response. They are detecting you're a bot, try some of these: headless=False, browser='webkit', consider using a proxy")

            try:
                return result
            except Exception as e:
                if retry_count == retries:
                    self.logger.error(f"Failed to return response: {result}")
                    raise e

                self.logger.info(
                    f"Failed a request, retrying ({retry_count}/{retries})"
                )
                if exponential_backoff:
                    await asyncio.sleep(2**retry_count)
                else:
                    await asyncio.sleep(1)

    async def close_sessions(self):
        """Close all the sessions. Should be called when you're done with the PlaywrightHelper object"""
        for session in self.sessions:
            await session.page.close()
            await session.context.close()
        self.sessions.clear()

    async def stop_playwright(self):
        """Stop the playwright browser"""
        await self.browser.close()
        await self.playwright.stop()

    async def get_session_content(self, url: str, **kwargs):
        """Get the content of a url"""
        _, session = self._get_session(**kwargs)
        return await session.page.content()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close_sessions()
        await self.stop_playwright()