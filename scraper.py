import requests
import sys
from lxml import etree
import re
import os
from datetime import date, datetime
import scraperwiki
from slackclient import SlackClient
from os import environ
import hashlib
import simplejson as json
from hashlib import md5
from lxml.html.clean import Cleaner
import json


def upload_question(question_json, legco_api_token):
    headers = {'Content-Type': 'application/json', 'Authorization': 'Token ' + legco_api_token}
    r = requests.put("https://api.g0vhk.io/legco/upsert_question", json=question_json, headers=headers)
    try:
        j = r.json()
    except Exception as e:
        print(r.text)
        raise e
    return j.get('created', False)


def all_text(node):
    return "".join([x for x in node.itertext()])


def crawl(token, channel, legco_api_token, year):
    if year == 0:
        today = date.today()
        year = today.year
        if today.month < 10:
            year = year - 1
    current_year = year - 2000
    print(current_year)
    year_start = (current_year // 4) * 4
    year_end = year_start + 4
    url = "https://www.legco.gov.hk/yr%.2d-%.2d/chinese/counmtg/question/ques%.2d%.2d.htm" % (current_year, current_year + 1, current_year, current_year + 1)
    print(url)
    r = requests.get(url)
    r.encoding = "utf-8"
    root = etree.HTML(r.text)
    dates = [d.text for d in root.xpath("//h2[@class=\"h3_style\"]/a[contains(@href,\"agenda\")]")]
    tables = root.xpath("//table[@class=\"interlaced\"]")
    if len(dates) != len(tables):
        raise Exception("Dates and Questions Mismatch! %d <> %d" % (len(dates), len(tables)) )    
    questions = []
    for i in range(0, len(dates)):
        question_date = datetime.strptime(dates[i], '%d.%m.%Y').strftime('%Y-%m-%d')
        print(question_date)
        table = tables[i]
        for row in table.xpath(".//tr")[1:]:
            cells = row.xpath("td")
            if all_text(cells[3]).strip() == '-':
                continue
            legislator_name = cells[1].text
            if legislator_name.startswith(u"郭偉强"):
                legislator_name = u"郭偉強"
            title = all_text(cells[2])
            question_type_text = all_text(cells[0])
            link_cells = cells[3].xpath(".//a")
            if len(link_cells) == 0:
                continue
            link = link_cells[0].attrib['href']
            key = str(hashlib.md5(link.encode('utf-8')).hexdigest())
            m = re.match(r"(.*[0-9]+|UQ)[\(]{0,1}(.*)\)", question_type_text)
            if m is None:
                raise Exception("Undefined Question Type", link, question_type_text)
            question_type = m.group(2)
            detail_r = requests.get(link)
            detail_r.encoding = "big5"
            output = detail_r.text
            cleaner = Cleaner(comments=False)
            output = cleaner.clean_html(output)
            detail_root = etree.HTML(output)
            try:
                press_release = all_text(detail_root.xpath("//div[@id=\"pressrelease\"]")[0])
            except IndexError:
                detail_r = requests.get(link)
                detail_r.encoding = "utf-8"
                output = detail_r.text
                output = cleaner.clean_html(output)
                detail_root = etree.HTML(output)
                print(link)
                press_release = all_text(detail_root.xpath("//span[@id=\"pressrelease\"]")[0])
            question_start = press_release.find(u'以下')
            reply_start = press_release.rfind(u'答覆：')
            question_text = press_release[question_start:reply_start]
            answer_text = press_release[reply_start + 3:]   
            question_dict = {
                'key': key,
                'individual': legislator_name,
                'date': question_date,
                'question_type': question_type,
                'question': question_text,
                'answer': answer_text,
                'title': title,
                'link': link,
                'title_ch': title
            }
            questions.append(question_dict)

    slack = SlackClient(token)
    for q in questions:
        key = q['key']
        existed = False
        try:
            existed = len(scraperwiki.sqlite.select('* from swdata where key = "%s"' % key)) > 0
        except:
            pass
        if existed:
            continue
        scraperwiki.sqlite.save(unique_keys=['key'], data={k: q[k] for k in ['key', 'date', 'link', 'individual']})
        created = upload_question(q, legco_api_token)
        if created:
            text = "New question is available at %s." % (q['link'])
            if channel:
                slack.api_call(
                        "chat.postMessage",
                        channel=channel,
                        text=text
                )
            else:
                print('Skipping Slack')
        else:
            print('Already uploaded')

TOKEN = environ.get('MORPH_TOKEN', None)
CHANNEL = environ.get('MORPH_CHANNEL', None)
LEGCO_API_TOKEN = environ.get('MORPH_LEGCO_API_TOKEN', None)
YEAR = int(environ.get('MORPH_YEAR', '0'))
crawl(TOKEN, CHANNEL, LEGCO_API_TOKEN, YEAR)
