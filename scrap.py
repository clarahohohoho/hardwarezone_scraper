import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import random

def thread_scrape(url, thread_list):

    url = 'https://forums.hardwarezone.com.sg/'
    page = requests.get(url)

    soup = BeautifulSoup(page.content, 'html.parser')

    for link in soup.findAll('a'): ##all links have 'a' as tag

        thread_list.append(url + link.get('href'))

    return thread_list

def page_scrape(url, thread, page_list):

    page = requests.get(thread)
    soup = BeautifulSoup(page.content, 'html.parser')

    for link in soup.findAll('a'): ##only pull links with id as thread or show
        if link.get('id'):
            if link.get('id').split('_')[0] == 'thread':
                page_list.append(url + link.get('href'))
        
        if link.get('title'):
            if link.get('title').split(' ')[0] == 'Show':
                page_list.append(url + link.get('href'))

    return list(set(page_list)) ##get unique links in list

def corpus_scrape(page_link, df, skipped):

    page = requests.get(page_link)
    soup = BeautifulSoup(page.content, 'html.parser')

    try: #try catch block implemented to prevent stopping of scraping operation due to bad page

        for link in soup.findAll(class_='post_message'): #find all posted messages
            new_row = {}
            new_row['page_url'] = page_link #store page url
            new_row['category'] = page_link.split('//')[2].split('/')[0].translate(str.maketrans('','','1234567890')) #get page catgory
            new_row['corpus'] = BeautifulSoup(str(link), "lxml").text #get posted messages
            df = df.append(new_row, ignore_index = True) #append into df

    except:

        skipped += 1 #add to skipped pages
        print('failed to read ', skipped , ' pages.')

    return df, skipped


def main():

    url = 'https://forums.hardwarezone.com.sg/'

    thread_list = []
    page_list = []
    page_listb = []

    counter = 0
    skipped = 0

    thread_list = thread_scrape(url, thread_list) #scrape all thread links in main page

    df = pd.DataFrame({'page_url':[], 'category':[], 'corpus':[]}) #initialise df

    while thread_list and len(df) < 1000000: #loop thru till thread_list is exhausted or when df reaches 1mil entries
        random.shuffle(thread_list) #randomly shuffle through thread_list
        thread = thread_list.pop(0) #remove current thread from thread_list
        print('reading thread:', thread)
        print('length of thread_list', len(thread_list))
        page_list = page_scrape(url, thread, page_list) #scrape thread for forum entries. if no forum entries found, goes to next thread.

        while page_list: #loop through till page_list is exhausted
            page = page_list.pop(0) #remove current page from page_list
            print('length of corpus:', len(df)) #print length of df
            print('length of page list', len(page_list)) #print how many pages are there on this thread
            check = ''.join([i for i in page.split('/')[-1].split('.')[0] if not i.isdigit()])

            if check == 'index': #if it is a next page link
                print('appending thread')
                thread_list.append(page) #append page back to thread list

            elif len(df) >= 1000000: #if df rches 1 mil records
                print('saving corpus..')
                df.to_csv('corpus3.csv')
                break
            
            else:
                if counter == 100: #if number of read pages rchs 100, saves df
                    print('saving corpus..')
                    df.to_csv('corpus3.csv')
                    counter = 0 #initialise page counter
                    print('counter:', counter)

                print('reading page:', page)
                counter += 1 #add 1 to read pages counter
                print('read pages counter:', counter)
                df, skipped = corpus_scrape(page, df, skipped) #scrape for posted messsages


if __name__ == "__main__":
    main()

