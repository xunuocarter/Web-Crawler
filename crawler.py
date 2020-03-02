import logging
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from collections import defaultdict
import lxml
from lxml import html



logger = logging.getLogger(__name__)

class Crawler:
    
    stop_word=["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as",
             "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't",
             "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
             "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
             "having", "he", "he'd", "he'll", "he's", "her", "here","here's", "hers", "herself", "him", "himself",
             "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
             "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off",
             "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same",
             "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that",
             "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they",
             "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up",
             "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's",
             "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with",
             "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
             "yourselves"]

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.common_word=defaultdict(int)
        self.out_Links = 0
        self.with_most_url=""
        self.max_count = 0
        self.with_most_words =""
        self.subdomains = {}
        self.trap = set()
        self.downloaded =set()



    def start_crawling(self):
        
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    self.downloaded.add(next_link)
                    #f1.write(str(next_link)+"\n")
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
        f1 = open("url.txt", "w")
        f1.write("total url number: " + str(len(self.downloaded))+"\n")
        for i in self.downloaded:
            f1.write(str(i)+"\n")
        f1.close()


        #write into the file
        f2 = open("traps.txt","w")
        f2.write("total traps number: " + str(len(self.trap))+"\n")
        for k in self.trap:
            f2.write(str(k)+"\n")
        f2.close()

        f3 = open("final_answer.txt","w")
        f3.write("Number of urls based on subdomain: \n")
        for j in self.subdomains:
            f3.write(str(j) + "\n")

        f3.write("Most valid word: \n")
        f3.write(str(self.with_most_url) + "\n")
        f3.write("\n")

        f3.write("Download urls: \n")
        f3.write("see url.txt")
        f3.write("\n")

        f3.write("Trap urls: \n")
        f3.write("see traps.txt")
        f3.write("\n")

        f3.write("Longest page: \n")
        f3.write(str(self.with_most_url) + "\n")
        f3.write("\n")

        f3.write("50 common words: ")
        f3.write(str(sorted(sorted(self.common_word.items(), key = lambda x:-x[1])[0:50])) + "\n")
        f3.close()




    def extract_next_links(self, url_data):
        
        outputLinks = []
        word_count=0
        try:
            get_data = html.fromstring(url_data["content"])  # get all element objects from the content
            find_link = get_data.xpath('//a/@href')  # find links from the content
            subdomain = urlparse(url_data["url"])
            subdomainName = subdomain.hostname
            for link in find_link:
                if subdomainName in self.subdomains:
                    self.subdomains[subdomainName] += 1
                else:
                    self.subdomains[subdomainName] = 1
                url = urljoin(url_data['url'], link)
                outputLinks.append(url)
            soup = BeautifulSoup(url_data["content"], 'html.parser')
            text = soup.get_text().split()
            for word in text:
                if word.lower() not in self.stop_word and re.match("[A-Za-z0-9]{2,}", word):
                    if word.lower() in self.common_word:
                        self.common_word[word.lower()] += 1
                    else:
                        self.common_word[word.lower()] = 1
                    word_count += 1
        except:
            pass

        #2 most vaild output
        if len(outputLinks) > self.out_Links:
            self.out_Links = len(outputLinks)
            self.with_most_url = url_data["url"]
        
        
        #4 longest page
        if word_count > self.max_count:
            self.with_most_words = url_data["url"]
            self.words_in_page = word_count
            
        return outputLinks

    def is_valid(self, url):
        
        if len(url) > 200:
            self.trap.add(url)
            return False
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            self.trap.add(url)
            return False

        new_url_list = url.split('/')

        count_freq = {}
        for item in new_url_list:
            if item in count_freq:
                count_freq[item] += 1
            else:
                count_freq[item] = 1

        sort_freq = sorted(count_freq.items(), key=lambda x: (-x[1], x[0]))

        if sort_freq[0][1] > 1:
                self.trap.add(url)
                return False

        if re.match("^.*calendar.*$", url.lower()):
            return '?' not in new_url_list[-1]
        query_split = new_url_list[-1].split('=')
        if len(query_split[-1]) > 30:
            self.trap.add(url)
            return False
        
        #dynamic url
        #url contains ? = &
        #suffix not in [".aspx",".asp",.jsp,.php,.perl,.cgi]
        dynamic_list=[".aspx",".asp",".jsp",".php",".perl",".cgi"]
        if re.search("[?=&]+",url)!=None:
            if any(suffix in url for suffix in dynamic_list):
                self.trap.add(url)
                return False

        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False



