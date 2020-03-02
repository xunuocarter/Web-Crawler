import atexit
import logging

import sys

from corpus import Corpus
from crawler import Crawler
from frontier import Frontier

if __name__ == "__main__":
    
    logging.basicConfig(format='%(asctime)s (%(name)s) %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    
    frontier = Frontier()
    frontier.load_frontier()

    
    corpus = Corpus(sys.argv[1])

    
    atexit.register(frontier.save_frontier)

    
    crawler = Crawler(frontier, corpus)
    crawler.start_crawling()
