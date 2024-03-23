import os
import json
import re
import queue
import math
import urllib.robotparser
import urllib.request
from progressbar import ProgressBar, Bar, Percentage
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time


def initialize_log():
    log_dir = "./log/"
    output_dir = "./output/"
    os.makedirs(log_dir, exist_ok=True) 
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs("./output/docs/", exist_ok=True)
    log_files = ["canonicalization", "wave_score", "error", "not_allowed", "current_links", "log"]
    for file_name in log_files:
        path = "./log/{}.txt".format(file_name)
        if os.path.exists(path):
            os.remove(path)

    output_files = ["out_links.json", "documents.txt", "in_links.json", "final.txt", "raw_html.json", "all_links.txt",
                    "crawled_links.txt"]
    for file_name in output_files:
        path = "./output/{}".format(file_name)
        if os.path.exists(path):
            os.remove(path)


def write_canonicalization(url, processed_url):
    with open("./log/canonicalization.txt", "a", encoding="utf-8") as f:
        f.write("{0},    {1}\n".format(url, processed_url))


def write_ap(id, url: str, text: str, header: dict, title=None):
    filename = f"document_{id}.txt"
    file_path = os.path.join("./output/docs/", filename)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("<DOC>\n")
        f.write("<DOCNO>{}</DOCNO>\n".format(url))
        if title is not None:
            f.write("<HEAD>{}</HEAD>\n".format(title))
        f.write("<HEADER>{}</HEADER>\n".format(json.dumps(header)))
        f.write("<TEXT>\n")
        f.write(text + "\n")
        f.write("</TEXT>\n")
        f.write("</DOC>\n")


def write_raw_html(raw_html: dict):
    with open("./output/raw_html.json", "a") as f:
        json.dump(raw_html, f)
        f.write("\n")


def write_wave_score(wave, wave_score: list):
    with open("./log/wave_score.txt", "a", encoding="utf-8") as f:
        for line in wave_score:
            f.write("{0}, {1}, {2}\n".format(wave, line[1], line[0]))


def write_all_out_links(out_links: dict):
    with open("./output/out_links.json", "a", encoding="utf-8") as f:
        json.dump(out_links, f)
        f.write("\n")


def write_all_in_links(in_links: dict):
    with open("./output/in_links.json", "a", encoding="utf-8") as f:
        json.dump(in_links, f)
        f.write("\n")
        
        
def write_error_info(error: str):
    with open("./log/error.txt", "a", encoding="utf-8") as f:
        f.write(error)


def write_not_allowed(not_allowed: str):
    with open("./log/not_allowed.txt", "a", encoding="utf-8") as f:
        f.write(not_allowed)


def write_final_info(crawled_links: int, found_links: int):
    with open("./output/final.txt", "w", encoding="utf-8") as f:
        f.write("Number of crawled links: {0}, Number of discovered links: {1}\n".format(crawled_links, found_links))


def write_current_link(url: str):
    with open("./log/current_links.txt", "a", encoding="utf-8") as f:
        f.write("{}\n".format(url))


def write_log(count, wave, url, score):
    with open("./log/log.txt", "a", encoding="utf-8") as f:
        f.write("{0}, {1}, {2}, {3}\n".format(count, wave, url, score))


def write_all_links(all_links):
    with open("./output/all_links.txt", "a", encoding="utf-8") as f:
        for line in all_links:
            f.write(line)
            f.write("\n")


def write_crawled_links(url):
    with open("./output/crawled_links.txt", "a") as f:
        f.write(url)
        f.write("\n")


class Canonicalizer:

    def get_domain(self, url: str):
        domain = re.findall("//[^/]*\w", url, flags=re.IGNORECASE)[0]
        domain = domain[2:]
        return domain

    def canonicalize(self, base_url: str, domain: str, url: str):
        if "\\" in url:
            result = re.sub("\\\\+", "/", url.encode("unicode_escape").decode())
        else:
            result = url
        result = re.sub("[\n\t ]*", "", result)
        try:
            # exception
            if domain == "www.vatican.va" or domain == "www.ysee.gr" or domain == "www.biblegateway.com":
                return ""
            if domain == "web.archive.org" and "en/member-churches" in result:
                return ""
            if domain == "penelope.uchicago.edu" and "E/Roman/Texts" in result:
                result = re.sub("E/.*", result, base_url)

            # remove anchor
            result = re.sub("#.*", "", result)
            if not re.findall("\w", result):
                return ""

            # handle something like "xxxx.html"
            if re.match("^[\w~]+[^:]*$", result):
                result = re.sub("/\w*[^/]*\w*$", "/" + result, base_url)
            elif re.match("^\w+[^/]+\w$", result):
                result = re.sub("/\w*[^/]*\w*$", "/" + result, base_url)
            elif re.match("^\./\w+[^:]*[\w/]$", result):
                result = re.sub("/\w*[^/]*\w*$", result[1:], base_url)
            elif re.match("^\?[^/]*", result):
                result = base_url + result

            # relative path ../../../ssss.ssd
            if re.match("^(?:\.{2}/)+\w+.*", result):
                replace = re.findall("\.{2}/\w+.*", result)[0][2:]
                level = len(re.findall("\.{2}", result))
                folders = re.findall("/\w+(?:\.\w+)*", base_url)
                target = "".join(folders[-level-1:])
                result = re.sub(target, replace, base_url)
            # non html
            black_list = [".jpg", ".svg", ".png", ".pdf", ".gif",
                          "youtube", "edit", "footer", "sidebar", "cite",
                          "special", "mailto", "books.google", "tel:",
                          "javascript", "www.vatican.va", ".ogv", "amazon",
                          ".webm"]
            for key in black_list:
                if key in result.lower():
                    return ""

            # remove port
            if re.match("https", result, flags=re.IGNORECASE) is not None:
                result = re.sub(":443", "", result)
            elif re.match("http", result, flags=re.IGNORECASE) is not None:
                result = re.sub(":80", "", result)
            # http/https case
            result = re.sub("http", "http", result, flags=re.IGNORECASE)
            result = re.sub("https", "http", result, flags=re.IGNORECASE)

            # missing domain
            if re.match("^/.+", result) is not None:
                result = "http://" + domain + result
            # missing protocal
            elif re.match("^//.+", result) is not None:
                result = "http:" + result
            # multiple slashes
            duplicate_slashes = re.findall("\w//+.", result)
            if len(duplicate_slashes) != 0:
                for dup in duplicate_slashes:
                    replace_str = dup[0] + "/" + dup[-1]
                    result = re.sub(dup, replace_str, result)

            # domain lower case
            find_domain = re.findall("//[^/]*\w", result)
            find_domain = find_domain[0]
            lower_case_domain = find_domain.lower()
            result = re.sub(find_domain, lower_case_domain, result)

            # convert empty path
            if re.match(".*com$", result) is not None:
                result += "/"

            # convert % triplets to upper case, (%7E, "~")
            percent_code = re.findall("%\w{2}", result)
            for p in percent_code:
                result = re.sub(p, p.upper(), result)
            return result
        except Exception as e:
            error = "Canonicalization error:\nbase_url = '{0}'\nurl = '{1}'\ndomain = '{2}'\n{3}\n\n".format(
                base_url, url, domain, str(e))
            print(error)
            write_error_info(error)
            return ""
        
class Frontier:

    def __init__(self):
        self.queue = queue.PriorityQueue()
        self.objects = {}
        self.waves = {}

    def initialize(self, seed_urls):
        self.waves[0] = set()
        for url in seed_urls:
            frontier_item = FrontierItem(url)
            frontier_item.compute_score()
            self.objects[url] = frontier_item
            self.waves[0].add(url)
            self.queue.put((0, frontier_item.score, url))
    
    def initialize_rest(self, rest_urls: list):
        for q in rest_urls:
            self.queue.put(q)

    def frontier_pop(self):
        return self.queue.get()

    def frontier_put(self, frontier_item, wave):
        url = frontier_item.url
        self.objects[url] = frontier_item
        if wave not in self.waves:
            self.waves[wave] = set()
        self.waves[wave].add(url)

    def frontier_update_inlinks(self, url, in_link):
        self.objects[url].update_in_links(in_link)

    def is_empty(self):
        return self.queue.empty()

    def change_wave(self, wave):
        if wave not in self.waves:
            return
        examine = []
        cutoff = 1.04
        for url in self.waves[wave]:
            frontier_item = self.objects[url]
            frontier_item.compute_score()
            if frontier_item.score > cutoff:
                continue
            examine.append((frontier_item.score, url))
            self.queue.put((wave, frontier_item.score, url))
        examine.sort()
        write_wave_score(wave, examine)


class FrontierItem:

    def __init__(self, url: str, raw_url=None):
        self.url = url
        self.raw_url = raw_url
        self.key_words = \
            ["catholic", "church", "commandments", "catechism",
             "jesus", "christ", "bishop", "pope", "sacred", "sacrament",
             "saint", "peter", "god", "theology", "relig", "papacy",
             "vatican", "doctrin", "canonical", "roman", "holy",
             "cardinal", "heaven", "baptism", "see"]
        self.in_links = set()
        self.score = 0
        self.text = ""
        self.raw_html = ""

    def compute_score(self):
        # key word hits
        keyword_hits = 0
        for k in self.key_words:
            if self.raw_url is not None:
                if len(re.findall(k, self.raw_url, flags=re.IGNORECASE)) != 0:
                    keyword_hits += 1
            else:
                if len(re.findall(k, self.url, flags=re.IGNORECASE)) != 0:
                    keyword_hits += 1
        keyword_score = math.exp(-keyword_hits)
        in_links_score = math.exp(-len(self.in_links))
        self.score = keyword_score + in_links_score

    def update_in_links(self, url: str):
        self.in_links.add(url)

class TimeoutRobotFileParser(urllib.robotparser.RobotFileParser):
    def __init__(self, url='', timeout=3):
        super().__init__(url)
        self.timeout = timeout

    def read(self):
        """Reads the robots.txt URL and feeds it to the parser."""
        try:
            f = urllib.request.urlopen(self.url, timeout=self.timeout)
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                self.disallow_all = True
            elif err.code >= 400:
                self.allow_all = True
        else:
            raw = f.read()
            self.parse(raw.decode("utf-8").splitlines())


class Robots:

    def __init__(self, url):
        self.url = url
        self.r = self.initialize()
        self.delay = 1.0

        self.get_delay()

    def initialize(self):
        r = TimeoutRobotFileParser()
        r.set_url(self.url)
        r.read()
        return r

    def get_delay(self):
        delay = self.r.crawl_delay(useragent="*")
        if delay is not None:
            self.delay = delay

    def can_fetch(self, new_url):
        return self.r.can_fetch("*", new_url)
    

class Parser:

    def __init__(self):
        self.path = ""
        self.doc = "documents.txt"
        self.raw_html = "raw_html.json"
        self.in_links = "new_in_links.json"
        self.out_links = "new_out_links.json"
        self.count = 40000
        self.canonicalizer = Canonicalizer()

    def initialize(self, path: str):
        self.path = path

    def doc_parse(self):
        # needed objects
        docs = {}
        headers = {}
        doc = list()
        add_file_flag = 0
        txt_flag = 0
        with open(self.path + self.doc, "r", encoding="utf-8") as f:
            bar = ProgressBar(widgets=["Read docs: ", Bar(), Percentage()], maxval=self.count)
            bar.start()
            count = 0
            for line in f:
                line = line.strip()
                # file end
                if re.search("</DOC>", line):
                    add_file_flag = 0
                    docs[data_id] = ' '.join(doc)
                    headers[data_id] = data_header
                    doc = list()
                    count += 1
                    bar.update(count)
                    if count == self.count:
                        bar.finish()
                        return docs, headers
                # add lines to file
                if add_file_flag == 1:
                    # id
                    if re.search("</DOCNO>", line):
                        data_id = re.sub("(<DOCNO>)|(</DOCNO>)", "", line)
                    # header
                    if re.search("</HEADER>", line):
                        data_header = json.loads(re.sub("(<HEADER>)|(</HEADER>)", "", line))
                    # text
                    # text end
                    if re.search("</TEXT>", line):
                        txt_flag = 0
                    if txt_flag == 1:
                        doc.append(line)
                    # text start
                    if re.search("<TEXT>", line):
                        if re.search("[A-Z|a-z]*[a-z]", line):
                            doc.append(line[6:])
                        txt_flag = 1
                # file start
                if re.search("<DOC>", line):
                    add_file_flag = 1
            bar.finish()
        return docs, headers

    def title_parse(self):
        # needed objects
        docs = {}
        doc = list()
        add_file_flag = 0
        txt_flag = 0
        with open(self.path + self.doc, "r", encoding="utf-8") as f:
            bar = ProgressBar(widgets=["Read title: ", Bar(), Percentage()], maxval=self.count)
            bar.start()
            count = 0
            for line in f:
                line = line.strip()
                # file end
                if re.search("</DOC>", line):
                    add_file_flag = 0
                    docs[data_id] = ''.join(doc)
                    doc = list()
                    count += 1
                    bar.update(count)
                    if count == self.count:
                        bar.finish()
                        return docs
                # add lines to file
                if add_file_flag == 1:
                    # id
                    if re.search("</DOCNO>", line):
                        data_id = re.sub("(<DOCNO>)|(</DOCNO>)", "", line)
                    # title
                    # title end
                    if re.search("</HEAD>", line):
                        txt_flag = 0
                    if txt_flag == 1:
                        doc.append(line)
                    # title start
                    if re.search("<HEAD>", line):
                        if re.search("</HEAD>", line):
                            doc.append(re.sub("(<HEAD>)|(</HEAD>)", "", line))
                            txt_flag = 0
                        else:
                            txt_flag = 1
                # file start
                if re.search("<DOC>", line):
                    add_file_flag = 1
            bar.finish()
        return docs

    def html_parse(self, start, end):
        raw_html = {}
        count = 0
        with open(self.path + self.raw_html, "r", encoding="utf-8") as fh:
            bar = ProgressBar(widgets=["Read html: ", Bar(), Percentage()], maxval=end)
            bar.start()
            for line in fh:
                count += 1
                if count > end:
                    bar.finish()
                    return raw_html
                if count >= start:
                    raw_html.update(json.loads(line))
                bar.update(count)
            bar.finish()
        return raw_html

    def links_parse(self):
        in_links = {}
        out_links = {}
        count = 0
        with open(self.path + self.in_links, "r", encoding="utf-8") as fi:
            bar = ProgressBar(widgets=["Read in_links: ", Bar(), Percentage()], maxval=40000)
            bar.start()
            for line in fi:
                in_links.update(json.loads(line))
                count += 1
                bar.update(count)
            bar.finish()
        count = 0
        with open(self.path + self.out_links, "r", encoding="utf-8") as fo:
            bar = ProgressBar(widgets=["Read out_links: ", Bar(), Percentage()], maxval=40000)
            bar.start()
            for line in fo:
                out_links.update(json.loads(line))
                count += 1
                bar.update(count)
            bar.finish()
        with open(self.path + "crawled_links.txt", "a") as f:
            for url in in_links:
                f.write(url)
                f.write("\n")
        return in_links, out_links

    def reduce_to_domain(self, in_links, out_links, crawled_links):
        new_in_links = {}
        for url in in_links:
            original = in_links[url]
            after = set()
            for u in original:
                if u not in crawled_links:
                    print("Weird")
                    domain = self.canonicalizer.get_domain(u)
                    new_u = re.findall("http.*{}".format(domain), u)[0]
                    after.add(new_u)
                else:
                    after.add(u)
            new_in_links[url] = after
        new_out_links = {}
        for url in out_links:
            original = out_links[url]
            after = set()
            for u in original:
                u_s = "https://" + u[7:]
                if u not in crawled_links and u_s not in crawled_links:
                    domain = self.canonicalizer.get_domain(u)
                    after.add("http://" + domain)
                else:
                    if u in crawled_links:
                        after.add(u)
                    else:
                        after.add(u_s)
            new_out_links[url] = after
        return new_in_links, new_out_links
    

class Crawler:

    def __init__(self):
        self.seed_urls = None
        self.frontier = Frontier()
        self.canonicalizer = Canonicalizer()
        self.all_links = None
        self.crawled_links = set()
        self.count = 0
        self.all_out_links = {}
        self.redirected_map = {}
        self.robots = {}
        self.robots_delay = {}
        self.robots_timer = {}
        self.time_out = 3
        self.total_count = 30000

    def initialize(self, seed_urls):
        self.all_links = set(seed_urls)
        self.seed_urls = seed_urls
        self.frontier.initialize(seed_urls)

    def crawl_control(self):
        initialize_log()

        current_wave = 0
        while True:
            # if empty, move to next wave
            if self.frontier.is_empty():
                self.frontier.change_wave(current_wave+1)
            # if still empty, finished
            if self.frontier.is_empty():
                self.finish()
                return "Finished"
            current_wave, score, url = self.frontier.frontier_pop()

            # get protocol, domain
            domain = self.canonicalizer.get_domain(url)

            # check robots.txt
            if domain not in self.robots:
                try:
                    robots = Robots("http://" + domain + "/robots.txt")
                    self.robots[domain] = robots
                    if robots.delay > self.time_out:
                        self.robots_delay[domain] = self.time_out
                    else:
                        self.robots_delay[domain] = robots.delay
                    self.robots_timer[domain] = datetime.now()
                except Exception as e:
                    error = "Read robots.txt error:\n{0}\nError: {1}\n\n".format("http://" + domain + "/robots.txt", e)
                    write_error_info(error)
                    continue

            delay = self.robots_delay[domain]

            # check if can fetch
            if not self.robots[domain].can_fetch(url):
                not_allowed = "Not Allowed: {}\n".format(url)
                print(not_allowed)
                write_not_allowed(not_allowed)
                continue
            else:
                # politeness
                since_last_crawl = datetime.now() - self.robots_timer[domain]
                if since_last_crawl.total_seconds() < delay:
                    time.sleep(delay - since_last_crawl.total_seconds())
                print("Current: " + url)
                write_current_link(url)
                # print time interval
                # print((datetime.now() - self.robots_timer[domain]).total_seconds())

                # get page header
                try:
                    url_head = self.get_head(url)
                    if url_head.status_code == 404:
                        error = "Status error:\n{0}\nError code: {1}\n\n".format(url, url_head.status_code)
                        write_error_info(error)
                        continue
                except Exception as e:
                    error = "Read head error:\n{0}\nError: {1}\n\n".format(url, e)
                    write_error_info(error)
                    self.robots_timer[domain] = datetime.now()
                    continue
                header = dict(url_head.headers)

                # get content type
                if "content-type" in url_head.headers:
                    content_type = url_head.headers["content-type"]
                else:
                    content_type = "text/html"
                # crawl html type
                if "text/html" not in content_type:
                    continue
                else:
                    # read page
                    try:
                        soup, raw_html, base_url, lang = self.get_page(url)
                        self.robots_timer[domain] = datetime.now()
                        # whether we should crawl, language, black list
                        if not self.page_should_crawl(base_url, lang):
                            continue
                        # multiple redirected url
                        if base_url in self.crawled_links:
                            self.frontier.objects[base_url].in_links.update(self.frontier.objects[url].in_links)
                            error = "Multiple redirected URL:\nURL: {0}\nRedirected URL: {1}\n\n".format(url, base_url)
                            write_error_info(error)
                            continue
                        else:
                            self.crawled_links.add(base_url)
                            frontier_item = FrontierItem(base_url)
                            frontier_item.in_links = self.frontier.objects[url].in_links
                            self.frontier.objects[base_url] = frontier_item
                            self.redirected_map[url] = base_url
                    except Exception as e:
                        error = "Read page error:\n{0}\nError: {1}\n\n".format(url, e)
                        write_error_info(error)
                        self.robots_timer[domain] = datetime.now()
                        continue

                    raw_out_links = self.get_out_links(soup)
                    out_links = []

                    # write as ap format
                    text = self.extract_text(soup)
                    if len(soup.select("title")) != 0:
                        title = soup.select("title")[0].get_text()
                    else:
                        title = None
                    
                    write_raw_html({base_url: raw_html})

                    for link in raw_out_links:
                        processed_link = self.canonicalizer.canonicalize(base_url, domain, link)
                        write_canonicalization(link, processed_link)
                        # if link is not empty
                        if len(processed_link) != 0:
                            out_links.append(processed_link)
                            if processed_link not in self.all_links:
                                # new frontier item
                                frontier_item = FrontierItem(processed_link, link)
                                frontier_item.update_in_links(base_url)

                                self.frontier.frontier_put(frontier_item, current_wave+1)
                                self.all_links.add(processed_link)
                            else:
                                # update in links
                                if processed_link in self.redirected_map:
                                    redirected = self.redirected_map[processed_link]
                                    self.frontier.frontier_update_inlinks(redirected, base_url)
                                else:
                                    self.frontier.frontier_update_inlinks(processed_link, base_url)
                    write_all_out_links({base_url: out_links})
                self.count += 1
                write_ap(self.count, base_url, text, header, title)
                print(self.count, current_wave, url, score)
                write_log(self.count, current_wave, url, score)
                write_final_info(len(self.crawled_links), len(self.all_links))
                if self.count == self.total_count:
                    self.finish()
                    print("Finished")
                    return

    def finish(self):
        for url in self.crawled_links:
            write_crawled_links(url)
            write_all_in_links({url: list(self.frontier.objects[url].in_links)})
        write_all_links(self.all_links)

    def get_out_links(self, soup):
        a = soup.select('a')
        out_links = []
        for item in a:
            if item.get('href'):
                out_links.append(item['href'])
        return out_links

    def get_page(self, url: str):
        headers = {"Connection": "close"}
        res = requests.get(url=url, headers=headers, timeout=self.time_out)
        soup = BeautifulSoup(res.text, "lxml")
        try:
            if soup.select("html")[0].has_attr("lang"):
                lang = soup.select("html")[0]['lang']
            else:
                lang = "en"
        except Exception as e:
            error = "Read language error:\n{0}\nError: {1}\n\n".format(url, e)
            write_error_info(error)
            lang = "en"
        base_url = res.url
        return soup, res.text, base_url, lang

    def get_head(self, url: str):
        headers = {"Connection": "close"}
        head = requests.head(url=url, headers=headers, timeout=self.time_out, allow_redirects=True)
        return head

    def extract_text(self, soup: BeautifulSoup):
        output = ""
        text = soup.find_all("p")
        for t in text:
            new_t = t.get_text()
            new_t = re.sub("\n", "", new_t)
            new_t = re.sub("  +", " ", new_t)
            if len(new_t) == 0:
                continue
            output += "{} ".format(new_t)
        return output

    def page_should_crawl(self, base_url, lang):
        result = True
        # check language
        if "en" not in lang.lower():
            error = "Language error: {0}\nLanguage = {1}\n\n".format(base_url, lang)
            write_error_info(error)
            result = False
        # check black list
        black_list = [".jpg", ".svg", ".png", ".pdf", ".gif",
                      "youtube", "edit", "footer", "sidebar", "cite",
                      "special", "mailto", "books.google", "tel:",
                      "javascript", "www.vatican.va", ".ogv", "amazon",
                      ".webm"]
        block = 0
        key = ""
        for key in black_list:
            if key in base_url.lower():
                block = 1
                break
        if block == 1:
            error = "Page type error: {0}\nkeyword = {1}\n\n".format(base_url, key)
            write_error_info(error)
            result = False
        return result


class Node:

    def __init__(self, url: str):
        self.url = url
        self.raw_page = ""
        self.out_links = set()
        self.in_links = set()


# given seed URLs, our topic is "Catholic Church"
seed_urls = [
    "http://en.wikipedia.org/wiki/Cold_War",
    "http://www.historylearningsite.co.uk/coldwar.htm",
    "http://en.wikipedia.org/wiki/Cuban_Missile_Crisis",
    "https://www.jfklibrary.org/learn/education/teachers/curricular-resources/high-school-curricular-resources/the-cuban-missile-crisis-how-to-respond?gclid=Cj0KCQiAv6yCBhCLARIsABqJTjZSDc77zAgSV2TD6d90REoOnYWZ1T_6pC_iJ7UyHHvqqnQiqExnD20aAjcHEALw_wcB"
    "https://www.google.com/search?q=cuban+missile+crisis&oq=cuban+missile+crisis&aqs=chrome..69i57j0i20i263j0l8.985j0j4&sourceid=chrome&ie=UTF-8"
    "https://www.google.com/search?client=safari&rls=en&q=cuban+missile+crisis&ie=UTF-8&oe=UTF-8"
]

# crawler
crawler = Crawler()
crawler.initialize(seed_urls)
crawler.crawl_control()