import requests
import asyncio
import aiohttp
import json
import zipfile
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import os
import base64
from pyrogram import Client, filters
import sys
import re
import requests
import uuid
import random
import string
import hashlib
from flask import Flask
import threading
from pyrogram.types.messages_and_media import message
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait
from pyromod import listen
from pyromod.exceptions.listener_timeout import ListenerTimeout
from pyrogram.types import Message
import pyrogram
from pyrogram import Client, filters
from pyrogram.types import User, Message
from pyrogram.enums import ChatMemberStatus
from pyrogram.raw.functions.channels import GetParticipants
from config import api_id, api_hash, bot_token, auth_users
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
THREADPOOL = ThreadPoolExecutor(max_workers=1000)
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Bot credentials from environment variables (Render compatible)
API_ID = int(os.environ.get("API_ID", 35657366))
API_HASH = os.environ.get("API_HASH", "390e65e78871a950ec7ead3594e49a3c")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8547656163:AAFxNNgZNEijhf6FeM7_v8GEsCM80a4yjXo")

# Initialize Bot Globally (IMPORTANT FIX)
bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Anonymous PW player proxy — resolves MPD URLs to playable stream URLs
# Works with any valid PW token regardless of batch ownership
PW_PROXY = "https://anonymouspwplayer-907e62cf4891.herokuapp.com/pw"

def make_proxy_url(raw_url: str, token: str) -> str:
    """Wrap a PW MPD URL through the anonymous proxy to get a playable URL.
    Correct format (from leechbot source): /pw?url={url}?token={token}
    The token is appended to the url param with ? not as a separate &token= param.
    """
    if not raw_url or not token:
        return raw_url
    return f"{PW_PROXY}?url={raw_url}?token={token}"

# Flask app for Render
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    app.run(host="0.0.0.0", port=1000) #Use 8080 Port here, if you're deploying it on koyeb
    

image_list = [
"https://graph.org/file/8b1f4146a8d6b43e5b2bc-be490579da043504d5.jpg",
"https://graph.org/file/b75dab2b3f7eaff612391-282aa53538fd3198d4.jpg",
"https://graph.org/file/38de0b45dd9144e524a33-0205892dd05593774b.jpg",
"https://graph.org/file/be39f0eebb9b66d7d6bc9-59af2f46a4a8c510b7.jpg",
"https://graph.org/file/8b7e3d10e362a2850ba0a-f7c7c46e9f4f50b10b.jpg",
]
print(4321)

def generate_pw_html(batch_name: str, json_data: dict) -> str:
    """Generate a rich HTML file from the batch JSON data — videos, notes, DPPs all clickable."""
    rows = []
    serial = 1

    batch = json_data.get(batch_name, {})
    for subject_name, chapters in batch.items():
        rows.append(f'''
        <tr class="subject-row">
            <td colspan="3">📚 {subject_name}</td>
        </tr>''')
        for chapter_name, content_types in chapters.items():
            rows.append(f'''
        <tr class="chapter-row">
            <td colspan="3">📖 {chapter_name}</td>
        </tr>''')
            for ctype, items in content_types.items():
                icon = "🎬" if "video" in ctype.lower() else ("📄" if "note" in ctype.lower() else "📝")
                label = ctype
                for item in items:
                    try:
                        name, url = item.split(":", 1)
                    except ValueError:
                        continue
                    rows.append(f'''
        <tr>
            <td>{serial}</td>
            <td>{icon} [{label}] {name}</td>
            <td><a href="{url}" target="_blank">▶ Open</a></td>
        </tr>''')
                    serial += 1

    rows_html = "\n".join(rows)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{batch_name}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: Arial, sans-serif; background: #0f0f0f; color: #e0e0e0; padding: 16px; }}
  h1 {{ text-align: center; color: #ff6b35; margin-bottom: 8px; font-size: 1.4em; }}
  .subtitle {{ text-align: center; color: #888; font-size: 0.85em; margin-bottom: 20px; }}
  input#search {{
    width: 100%; padding: 10px 14px; border-radius: 8px;
    border: 1px solid #333; background: #1e1e1e; color: #fff;
    font-size: 0.95em; margin-bottom: 16px; outline: none;
  }}
  table {{ width: 100%; border-collapse: collapse; }}
  th {{ background: #1a1a1a; color: #ff6b35; padding: 10px 8px; text-align: left; font-size: 0.85em; position: sticky; top: 0; }}
  td {{ padding: 8px; border-bottom: 1px solid #1f1f1f; font-size: 0.82em; vertical-align: middle; }}
  tr:hover td {{ background: #1a1a1a; }}
  .subject-row td {{ background: #1f1220; color: #ff6b35; font-weight: bold; font-size: 0.9em; padding: 10px 8px; }}
  .chapter-row td {{ background: #141414; color: #aaa; font-size: 0.8em; padding: 6px 8px 6px 20px; }}
  a {{ color: #4fc3f7; text-decoration: none; font-weight: bold; }}
  a:hover {{ color: #fff; }}
  td:first-child {{ width: 40px; color: #555; text-align: center; }}
  td:last-child {{ width: 80px; text-align: center; }}
  .hidden {{ display: none; }}
</style>
</head>
<body>
<h1>{batch_name}</h1>
<p class="subtitle">Total {serial - 1} items — Open links in Chrome, copy URL from address bar to download</p>
<input type="text" id="search" placeholder="🔍 Search lectures, notes, DPPs..." oninput="filterTable()">
<table>
  <thead><tr><th>#</th><th>Content</th><th>Link</th></tr></thead>
  <tbody id="tableBody">
{rows_html}
  </tbody>
</table>
<script>
function filterTable() {{
  const q = document.getElementById("search").value.toLowerCase();
  document.querySelectorAll("#tableBody tr").forEach(row => {{
    if (row.classList.contains("subject-row") || row.classList.contains("chapter-row")) return;
    row.classList.toggle("hidden", !row.textContent.toLowerCase().includes(q));
  }});
}}
</script>
</body>
</html>'''



@bot.on_message(filters.command(["start"]))
async def start(bot, message):
  random_image_url = random.choice(image_list)

  keyboard = [
    [
      InlineKeyboardButton("🚀 Physics Wallah Extractor 🚀", callback_data="pwwp")
    ],
    [
      InlineKeyboardButton("🆓 PW Free Extract (No Login) 🆓", callback_data="pwfree")
    ]
  ]

  reply_markup = InlineKeyboardMarkup(keyboard)

  await message.reply_photo(
    photo=random_image_url,
    caption="**PLEASE👇PRESS👇HERE**",
    quote=True,
    reply_markup=reply_markup
  )
@bot.on_message(group=2)
#async def account_login(bot: Client, m: Message):
#    try:
#        await bot.forward_messages(chat_id=chat_id, from_chat_id=m.chat.id, message_ids=m.id)
#    except:
#        pass
        
async def fetch_pwwp_data(session: aiohttp.ClientSession, url: str, headers: Dict = None, params: Dict = None, data: Dict = None, method: str = 'GET') -> Any:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with session.request(method, url, headers=headers, params=params, json=data) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logging.error(f"Attempt {attempt + 1} failed: aiohttp error fetching {url}: {e}")
        except Exception as e:
            logging.exception(f"Attempt {attempt + 1} failed: Unexpected error fetching {url}: {e}")

        if attempt < max_retries - 1:
            await asyncio.sleep(90 ** attempt)
        else:
            logging.error(f"Failed to fetch {url} after {max_retries} attempts.")
            return None


async def process_pwwp_chapter_content(session: aiohttp.ClientSession, chapter_id, selected_batch_id, subject_id, schedule_id, content_type, headers: Dict, raw_url: str = '', raw_topic: str = ''):
    content = []

    if content_type in ("videos", "DppVideos"):
        # Extract token from headers for proxy use
        token = (headers.get('authorization') or headers.get('Authorization') or '').replace('Bearer ', '').strip()

        if raw_url:
            name = raw_topic or schedule_id
            # If MPD URL, wrap through proxy so it resolves to playable stream
            if 'd1d34p8vz63oiq' in raw_url or '.mpd' in raw_url:
                out_url = make_proxy_url(raw_url, token) if token else raw_url
            else:
                out_url = raw_url
            content.append(f"{name}:{out_url}")
            return {content_type: content} if content else {}

        # raw_url empty — hit schedule-details to get videoUrl
        det_url = f"https://api.penpencil.co/v1/batches/{selected_batch_id}/subject/{subject_id}/schedule/{schedule_id}/schedule-details"
        data = await fetch_pwwp_data(session, det_url, headers=headers)
        if data and data.get("success") and data.get("data"):
            data_item = data["data"]
            name = data_item.get('topic', '') or raw_topic or schedule_id
            video_details = data_item.get('videoDetails', {})
            if video_details:
                video_url = video_details.get('videoUrl') or video_details.get('embedCode') or ""
                if video_url:
                    if 'd1d34p8vz63oiq' in video_url or '.mpd' in video_url:
                        video_url = make_proxy_url(video_url, token) if token else video_url
                    content.append(f"{name}:{video_url}")
                    return {content_type: content} if content else {}
            top_url = data_item.get('url', '')
            if top_url and 'cloudfront' in top_url:
                if 'd1d34p8vz63oiq' in top_url or '.mpd' in top_url:
                    top_url = make_proxy_url(top_url, token) if token else top_url
                content.append(f"{name}:{top_url}")
        return {content_type: content} if content else {}

    else:
        # notes / DppNotes — still need schedule-details for attachments
        url = f"https://api.penpencil.co/v1/batches/{selected_batch_id}/subject/{subject_id}/schedule/{schedule_id}/schedule-details"
        data = await fetch_pwwp_data(session, url, headers=headers)

        if data and data.get("success") and data.get("data"):
            data_item = data["data"]
            homework_ids = data_item.get('homeworkIds', [])
            for homework in homework_ids:
                attachment_ids = homework.get('attachmentIds', [])
                name = homework.get('topic', '')
                for attachment in attachment_ids:
                    att_url = attachment.get('baseUrl', '') + attachment.get('key', '')
                    if att_url:
                        content.append(f"{name}:{att_url}")

        return {content_type: content} if content else {}


async def fetch_pwwp_all_schedule(session: aiohttp.ClientSession, chapter_id, selected_batch_id, subject_id, content_type, headers: Dict) -> List[Dict]:
    all_schedule = []
    page = 1
    while True:
        params = {
            'tag': chapter_id,
            'contentType': content_type,
            'page': page
        }
        url = f"https://api.penpencil.co/v2/batches/{selected_batch_id}/subject/{subject_id}/contents"
        data = await fetch_pwwp_data(session, url, headers=headers, params=params)

        if data and data.get("success") and data.get("data"):
            for item in data["data"]:
                item['content_type'] = content_type
                # Skip PW announcement/update entries — they have no real video URL
                topic = (item.get('topic') or '').lower()
                skip_keywords = ['new feature update', 'new update', 'important update',
                                  'new study page', 'study page update', 'feature update']
                if content_type in ('videos', 'DppVideos') and any(k in topic for k in skip_keywords):
                    continue
                all_schedule.append(item)
            page += 1
        else:
            break
    return all_schedule


async def process_pwwp_chapters(session: aiohttp.ClientSession, chapter_id, selected_batch_id, subject_id, headers: Dict):
    content_types = ['videos', 'notes', 'DppNotes', 'DppVideos']
    
    all_schedule_tasks = [fetch_pwwp_all_schedule(session, chapter_id, selected_batch_id, subject_id, content_type, headers) for content_type in content_types]
    all_schedules = await asyncio.gather(*all_schedule_tasks)
    
    all_schedule = []
    for schedule in all_schedules:
        all_schedule.extend(schedule)
        
    content_tasks = [
        process_pwwp_chapter_content(session, chapter_id, selected_batch_id, subject_id, item["_id"], item['content_type'], headers, item.get('url', ''), item.get('topic', ''))
        for item in all_schedule
    ]
    content_results = await asyncio.gather(*content_tasks)

    combined_content = {}
    for result in content_results:
        if result:
            for content_type, content_list in result.items():
                if content_type not in combined_content:
                    combined_content[content_type] = []
                combined_content[content_type].extend(content_list)

    return combined_content

async def fetch_pwwp_subject_videos(session: aiohttp.ClientSession, selected_batch_id: str, subject_id: str, headers: Dict) -> List[str]:
    """Fetch ALL video URLs for a subject without chapter tag filter.
    This catches videos not yet tagged to any chapter (ongoing batches)."""
    video_lines = []
    seen_ids = set()
    skip_keywords = ['new feature update', 'new update', 'important update',
                     'new study page', 'study page update', 'feature update']
    for page in range(1, 200):
        params = {'page': str(page), 'tag': '', 'contentType': 'videos'}
        data = await fetch_pwwp_data(
            session,
            f"https://api.penpencil.co/v2/batches/{selected_batch_id}/subject/{subject_id}/contents",
            headers=headers, params=params
        )
        items = data.get("data", []) if data else []
        if not items:
            break
        for item in items:
            item_id = item.get("_id", "")
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            topic = (item.get("topic") or "").strip()
            raw_url = (item.get("url") or "").strip()
            if any(k in topic.lower() for k in skip_keywords):
                continue
            if raw_url:
                # Use proxy for MPD URLs
                token = (headers.get('authorization') or headers.get('Authorization') or '').replace('Bearer ', '').strip()
                if 'd1d34p8vz63oiq' in raw_url or '.mpd' in raw_url:
                    raw_url = make_proxy_url(raw_url, token) if token else raw_url
                video_lines.append(f"{topic}:{raw_url}")
    return video_lines


async def get_pwwp_all_chapters(session: aiohttp.ClientSession, selected_batch_id, subject_id, headers: Dict):
    all_chapters = []
    page = 1
    while True:
        url = f"https://api.penpencil.co/v2/batches/{selected_batch_id}/subject/{subject_id}/topics?page={page}"
        data = await fetch_pwwp_data(session, url, headers=headers)

        if data and data.get("data"):
            chapters = data["data"]
            all_chapters.extend(chapters)
            page += 1
        else:
            break

    return all_chapters


async def process_pwwp_subject(session: aiohttp.ClientSession, subject: Dict, selected_batch_id: str, selected_batch_name: str, zipf: zipfile.ZipFile, json_data: Dict, all_subject_urls: Dict[str, List[str]], headers: Dict):
    subject_name = subject.get("subject", "Unknown Subject").replace("/", "-")
    subject_id = subject.get("_id")
    json_data[selected_batch_name][subject_name] = {}
    zipf.writestr(f"{subject_name}/", "")
    
    chapters = await get_pwwp_all_chapters(session, selected_batch_id, subject_id, headers)
    
    chapter_tasks = []
    for chapter in chapters:
        chapter_name = chapter.get("name", "Unknown Chapter").replace("/", "-")
        zipf.writestr(f"{subject_name}/{chapter_name}/", "")
        json_data[selected_batch_name][subject_name][chapter_name] = {}

        chapter_tasks.append(process_pwwp_chapters(session, chapter["_id"], selected_batch_id, subject_id, headers))

    chapter_results = await asyncio.gather(*chapter_tasks)

    # Collect all chapter-tagged video URLs to dedup against subject-level fetch
    all_urls = []
    chapter_video_urls = set()
    for chapter, chapter_content in zip(chapters, chapter_results):
        chapter_name = chapter.get("name", "Unknown Chapter").replace("/", "-")

        for content_type in ['videos', 'notes', 'DppNotes', 'DppVideos']:
            if chapter_content.get(content_type):
                content = chapter_content[content_type]
                content.reverse()
                content_string = "\n".join(content)
                zipf.writestr(f"{subject_name}/{chapter_name}/{content_type}.txt", content_string.encode('utf-8'))
                json_data[selected_batch_name][subject_name][chapter_name][content_type] = content
                all_urls.extend(content)
                if content_type in ('videos', 'DppVideos'):
                    for line in content:
                        chapter_video_urls.add(line)

    # Fetch ALL subject videos without chapter filter — catches untagged/ongoing lectures
    subject_videos = await fetch_pwwp_subject_videos(session, selected_batch_id, subject_id, headers)

    # Add videos not already captured by chapter-tagged fetch
    extra_videos = [v for v in subject_videos if v not in chapter_video_urls]
    if extra_videos:
        extra_str = "\n".join(extra_videos)
        zipf.writestr(f"{subject_name}/_all_videos.txt", extra_str.encode('utf-8'))
        if json_data[selected_batch_name][subject_name].get('_all_videos') is None:
            json_data[selected_batch_name][subject_name]['_all_videos'] = {}
        json_data[selected_batch_name][subject_name]['_all_videos']['videos'] = extra_videos
        all_urls.extend(extra_videos)

    all_subject_urls[subject_name] = all_urls

def find_pw_old_batch(batch_search):

    try:
        response = requests.get(f"https://abhiguru143.github.io/AS-MULTIVERSE-PW/batch/batch.json")
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        return []

    matching_batches = []
    for batch in data:
        if batch_search.lower() in batch['batch_name'].lower():
            matching_batches.append(batch)

    return matching_batches

async def get_pwwp_todays_schedule_content_details(session: aiohttp.ClientSession, selected_batch_id, subject_id, schedule_id, headers: Dict) -> List[str]:

    url = f"https://api.penpencil.co/v1/batches/{selected_batch_id}/subject/{subject_id}/schedule/{schedule_id}/schedule-details"
    data = await fetch_pwwp_data(session, url, headers)
    content = []

    if data and data.get("success") and data.get("data"):
        data_item = data["data"]
        
        video_details = data_item.get('videoDetails', {})
        if video_details:
            name = data_item.get('topic')
            
            videoUrl = video_details.get('videoUrl') or video_details.get('embedCode')
            image = video_details.get('image')
                
            if videoUrl:
                line = f"{name}:{videoUrl}\n"
                content.append(line)
           #     logging.info(line)
               
                          
        homework_ids = data_item.get('homeworkIds')
        for homework in homework_ids:
            attachment_ids = homework.get('attachmentIds')
            name = homework.get('topic')
            for attachment in attachment_ids:
            
                url = attachment.get('baseUrl', '') + attachment.get('key', '')
                        
                if url:
                    line = f"{name}:{url}\n"
                    content.append(line)
                #    logging.info(line)
                
        dpp = data_item.get('dpp')
        if dpp:
            dpp_homework_ids = dpp.get('homeworkIds')
            for homework in dpp_homework_ids:
                attachment_ids = homework.get('attachmentIds')
                name = homework.get('topic')
                for attachment in attachment_ids:
                
                    url = attachment.get('baseUrl', '') + attachment.get('key', '')
                        
                    if url:
                        line = f"{name}:{url}\n"
                        content.append(line)
                    #    logging.info(line)
    else:
        logging.warning(f"No Data Found For  Id - {schedule_id}")
    return content
    
async def get_pwwp_all_todays_schedule_content(session: aiohttp.ClientSession, selected_batch_id: str, headers: Dict) -> List[str]:

    url = f"https://api.penpencil.co/v1/batches/{selected_batch_id}/todays-schedule"
    todays_schedule_details = await fetch_pwwp_data(session, url, headers)
    all_content = []

    if todays_schedule_details and todays_schedule_details.get("success") and todays_schedule_details.get("data"):
        tasks = []

        for item in todays_schedule_details['data']:
            schedule_id = item.get('_id')
            subject_id = item.get('batchSubjectId')
            
            task = asyncio.create_task(get_pwwp_todays_schedule_content_details(session, selected_batch_id, subject_id, schedule_id, headers))
            tasks.append(task)
            
        results = await asyncio.gather(*tasks)
        
        for result in results:
            all_content.extend(result)
            
    else:
        logging.warning("No today's schedule data found.")

    return all_content
    
@bot.on_callback_query(filters.regex("^pwwp$"))
async def pwwp_callback(bot, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    
    auth_user = auth_users[0]
    user = await bot.get_users(auth_user)
    owner_username = "@" + user.username

    if user_id not in auth_users:
        await bot.send_message(callback_query.message.chat.id, f"**You Are Not Subscribed To This Bot\nContact - {owner_username}**")
        return
            
    THREADPOOL.submit(asyncio.run, process_pwwp(bot, callback_query.message, user_id))

async def process_pwwp(bot: Client, m: Message, user_id: int):

    editable = await m.reply_text("**Enter Woking Access Token\n\nOR\n\nEnter Phone Number**")

    try:
        input1 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
        raw_text1 = input1.text
        await input1.delete(True)
    except:
        await editable.edit("**Timeout! You took too long to respond**")
        return

    headers = {
        'Host': 'api.penpencil.co',
        'client-id': '5eb393ee95fab7468a79d189',
        'client-version': '1910',
        'user-agent': 'Mozilla/5.0 (Linux; Android 12; M2101K6P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36',
        'randomid': '72012511-256c-4e1c-b4c7-29d67136af37',
        'client-type': 'WEB',
        'content-type': 'application/json; charset=utf-8',
    }

    loop = asyncio.get_event_loop()    
    CONNECTOR = aiohttp.TCPConnector(limit=1000, loop=loop)
    async with aiohttp.ClientSession(connector=CONNECTOR, loop=loop) as session:
        try:
            if raw_text1.isdigit() and len(raw_text1) == 10:
                phone = raw_text1
                data = {
                    "username": phone,
                    "countryCode": "+91",
                    "organizationId": "5eb393ee95fab7468a79d189"
                }
                try:
                    async with session.post(f"https://api.penpencil.co/v1/users/get-otp?smsType=0", json=data, headers=headers) as response:
                        await response.read()
                    
                except Exception as e:
                    await editable.edit(f"**Error : {e}**")
                    return

                editable = await editable.edit("**ENTER OTP YOU RECEIVED**")
                try:
                    input2 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                    otp = input2.text
                    await input2.delete(True)
                except:
                    await editable.edit("**Timeout! You took too long to respond**")
                    return

                payload = {
                    "username": phone,
                    "otp": otp,
                    "client_id": "system-admin",
                    "client_secret": "KjPXuAVfC5xbmgreETNMaL7z",
                    "grant_type": "password",
                    "organizationId": "5eb393ee95fab7468a79d189",
                    "latitude": 0,
                    "longitude": 0
                }

                try:
                    async with session.post(f"https://api.penpencil.co/v3/oauth/token", json=payload, headers=headers) as response:
                        access_token = (await response.json())["data"]["access_token"]
                        monster = await editable.edit(f"<b>Physics Wallah Login Successful ✅</b>\n\n<pre language='Save this Login Token for future usage'>{access_token}</pre>\n\n")
                        editable = await m.reply_text("**Getting Batches In Your I'd**")
                    
                except Exception as e:
                    await editable.edit(f"**Error : {e}**")
                    return

            else:
                access_token = raw_text1
            
            headers['authorization'] = f"Bearer {access_token}"
        
            params = {
                'mode': '1',
                'page': '1',
            }
            try:
                async with session.get(f"https://api.penpencil.co/v3/batches/all-purchased-batches", headers=headers, params=params) as response:
                    response.raise_for_status()
                    batches = (await response.json()).get("data", [])
            except Exception as e:
                await editable.edit("**```\nLogin Failed❗TOKEN IS EXPIRED```\nPlease Enter Working Token\n                       OR\nLogin With Phone Number**")
                return
        
            await editable.edit("**Enter Your Batch Name**")
            try:
                input3 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                batch_search = input3.text
                await input3.delete(True)
            except:
                await editable.edit("**Timeout! You took too long to respond**")
                return
                
            url = f"https://api.penpencil.co/v3/batches/search?name={batch_search}"
            courses = await fetch_pwwp_data(session, url, headers)
            courses = courses.get("data", {}) if courses else {}

            if courses:
                text = ''
                for cnt, course in enumerate(courses):
                    name = course['name']
                    text += f"{cnt + 1}. ```\n{name}```\n"
                await editable.edit(f"**Send index number of the course to download.\n\n{text}\n\nIf Your Batch Not Listed Above Enter - No**")
            
                try:
                    input4 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                    raw_text4 = input4.text
                    await input4.delete(True)
                except:
                    await editable.edit("**Timeout! You took too long to respond**")
                    return
                
                if input4.text.isdigit() and 1 <= int(input4.text) <= len(courses):
                    selected_course_index = int(input4.text.strip())
                    course = courses[selected_course_index - 1]
                    selected_batch_id = course['_id']
                    selected_batch_name = course['name']
                    clean_batch_name = selected_batch_name.replace("/", "-").replace("|", "-")
                    clean_file_name = f"{user_id}_{clean_batch_name}"
                    
                elif "No" in input4.text:
                    courses = find_pw_old_batch(batch_search)
                    if courses:
                        text = ''
                        for cnt, course in enumerate(courses):
                            name = course['batch_name']
                            text += f"{cnt + 1}. ```\n{name}```\n"
                            
                        await editable.edit(f"**Send index number of the course to download.\n\n{text}**")
                
                        try:
                            input5 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                            raw_text5 = input5.text
                            await input5.delete(True)
                        except:
                            await editable.edit("**Timeout! You took too long to respond**")
                            return
                
                        if input5.text.isdigit() and 1 <= int(input5.text) <= len(courses):
                            selected_course_index = int(input5.text.strip())
                            course = courses[selected_course_index - 1]
                            selected_batch_id = course['batch_id']
                            selected_batch_name = course['batch_name']
                            clean_batch_name = selected_batch_name.replace("/", "-").replace("|", "-")
                            clean_file_name = f"{user_id}_{clean_batch_name}"
                        else:
                            raise Exception("Invalid batch index.")
                else:
                    raise Exception("Invalid batch index.")
                    
                await editable.edit("1.```\nFull Batch```\n2.```\nToday's Class```\n3.```\nKhazana```\n4.```\nVideos Only```")
                    
                try:
                    input6 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                    raw_text6 = input6.text
                    await input6.delete(True)
                except ListenerTimeout:
                    await editable.edit("**Timeout! You took too long to respond**")
                    return
                except Exception as e:
                    logging.exception("Error during option listening:")
                    try:
                        await editable.edit(f"**Error: {e}**")
                    except:
                        logging.error(f"Failed to send error message to user: {e}")
                    return
                        
                await editable.edit(f"**Extracting course : {selected_batch_name} ...**")

                start_time = time.time()

                if input6.text == '1':
                
                    url = f"https://api.penpencil.co/v3/batches/{selected_batch_id}/details"
                    batch_details = await fetch_pwwp_data(session, url, headers=headers)

                    if batch_details and batch_details.get("success"):
                        subjects = batch_details.get("data", {}).get("subjects", [])

                        json_data = {selected_batch_name: {}}
                        all_subject_urls = {}

                        with zipfile.ZipFile(f"{clean_file_name}.zip", 'w') as zipf:
                            
                            subject_tasks = [process_pwwp_subject(session, subject, selected_batch_id, selected_batch_name, zipf, json_data, all_subject_urls, headers) for subject in subjects]
                            await asyncio.gather(*subject_tasks)
                        
                        with open(f"{clean_file_name}.json", 'w') as f:
                            json.dump(json_data, f, indent=4)
                            
                        with open(f"{clean_file_name}.txt", 'w', encoding='utf-8') as f:
                            for subject in subjects:
                                subject_name = subject.get("subject", "Unknown Subject").replace("/", "-")
                                if subject_name in all_subject_urls:
                                    f.write('\n'.join(all_subject_urls[subject_name]) + '\n')

                        # Generate HTML file
                        html_content = generate_pw_html(selected_batch_name, json_data)
                        with open(f"{clean_file_name}.html", 'w', encoding='utf-8') as f:
                            f.write(html_content)

                    else:
                        raise Exception(f"Error fetching batch details: {batch_details.get('message')}")
                    
                elif input6.text == '2':
                    
                    selected_batch_name = "Today's Class"
                    today_schedule = await get_pwwp_all_todays_schedule_content(session, selected_batch_id, headers)
                    if today_schedule:
                        with open(f"{clean_file_name}.txt", "w", encoding="utf-8") as f:
                            f.writelines(today_schedule)
                    else:
                        raise Exception("No Classes Found Today")
                        
                elif input6.text == '3':
                    raise Exception("Working In Progress")

                elif input6.text == '4':
                    # Videos Only using /v2/contents API + CloudFront HLS URL construction
                    url = f"https://api.penpencil.co/v3/batches/{selected_batch_id}/details"
                    batch_details = await fetch_pwwp_data(session, url, headers=headers)

                    if not (batch_details and batch_details.get("success")):
                        raise Exception("Error fetching batch details")

                    subjects = batch_details.get("data", {}).get("subjects", [])
                    all_video_lines = []

                    for subject in subjects:
                        subject_id = subject.get("_id")
                        for page in range(1, 100):
                            params_v = {
                                "page": str(page),
                                "tag": "",
                                "contentType": "videos",
                            }
                            res = await fetch_pwwp_data(
                                session,
                                f"https://api.penpencil.co/v2/batches/{selected_batch_id}/subject/{subject_id}/contents",
                                headers=headers,
                                params=params_v
                            )
                            data_list = res.get("data", []) if res else []
                            if not data_list:
                                break
                            for item in data_list:
                                try:
                                    raw_url = item.get("url", "")
                                    topic = item.get("topic", "Unknown")
                                    if raw_url:
                                        # Wrap MPD URLs through proxy for playable output
                                        token = (headers.get('authorization') or headers.get('Authorization') or '').replace('Bearer ', '').strip()
                                        if 'd1d34p8vz63oiq' in raw_url or '.mpd' in raw_url:
                                            raw_url = make_proxy_url(raw_url, token) if token else raw_url
                                        all_video_lines.append(f"{topic}:{raw_url}\n")
                                except Exception:
                                    pass

                    if all_video_lines:
                        with open(f"{clean_file_name}.txt", "w", encoding="utf-8") as f:
                            f.writelines(all_video_lines)

                        # Generate HTML for Videos Only
                        video_json = {selected_batch_name: {"Videos": {"All Videos": {"videos": [line.strip() for line in all_video_lines]}}}}
                        html_content = generate_pw_html(selected_batch_name, video_json)
                        with open(f"{clean_file_name}.html", "w", encoding="utf-8") as f:
                            f.write(html_content)
                    else:
                        raise Exception("No videos found in this batch")

                else:
                    raise Exception("Invalid index.")
                    
                end_time = time.time()
                response_time = end_time - start_time
                minutes = int(response_time // 60)
                seconds = int(response_time % 60)

                if minutes == 0:
                    if seconds < 1:
                        formatted_time = f"{response_time:.2f} seconds"
                    else:
                        formatted_time = f"{seconds} seconds"
                else:
                    formatted_time = f"{minutes} minutes {seconds} seconds"
                            
                await editable.delete(True)
                
                caption = f"**Batch Name : ```\n{selected_batch_name}``````\nTime Taken : {formatted_time}```**"
                        
                files = [f"{clean_file_name}.{ext}" for ext in ["txt", "zip", "json", "html"]]
                for file in files:
                    file_ext = os.path.splitext(file)[1][1:]
                    try:
                        with open(file, 'rb') as f:
                            doc = await m.reply_document(document=f, caption=caption, file_name=f"{clean_batch_name}.{file_ext}")
                    except FileNotFoundError:
                        logging.error(f"File not found: {file}")
                    except Exception as e:
                        logging.exception(f"Error sending document {file}:")
                    finally:
                        try:
                            os.remove(file)
                            logging.info(f"Removed File After Sending : {file}")
                        except OSError as e:
                            logging.error(f"Error deleting {file}: {e}")
            else:
                raise Exception("No batches found for the given search name.")
                
        except Exception as e:
            logging.exception(f"An unexpected error occurred: {e}")
            try:
                await editable.edit(f"**Error : {e}**")
            except Exception as ee:
                logging.error(f"Failed to send error message to user in callback: {ee}")
        finally:
            if session:
                await session.close()
            await CONNECTOR.close()
            
@bot.on_callback_query(filters.regex("^pwfree$"))
async def pwfree_callback(bot, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()

    auth_user = auth_users[0]
    user = await bot.get_users(auth_user)
    owner_username = "@" + user.username

    if user_id not in auth_users:
        await bot.send_message(callback_query.message.chat.id,
            f"**You Are Not Subscribed To This Bot\nContact - {owner_username}**")
        return

    THREADPOOL.submit(asyncio.run, process_pwfree(bot, callback_query.message, user_id))


async def process_pwfree(bot: Client, m: Message, user_id: int):
    loop = asyncio.get_event_loop()
    CONNECTOR = aiohttp.TCPConnector(limit=100, loop=loop)
    async with aiohttp.ClientSession(connector=CONNECTOR, loop=loop) as session:
        try:
            editable = await m.reply_text(
                "**🆓 PW Free Extract (No Login Required)**\n\n"
                "Send batch name to search:"
            )
            try:
                inp = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                search = inp.text.strip()
                await inp.delete(True)
            except ListenerTimeout:
                await editable.edit("**Timeout!**"); return

            await editable.edit(f"**🔍 Searching for: {search}...**")

            # Search batches
            data = await suarkafan_get(session, "getAllBatchesSearch.php", {"search": search})

            if not data or not isinstance(data, list) or len(data) == 0:
                await editable.edit(
                    "**❌ No batches found OR the free API is currently down.**\n\n"
                    "This API (`suarkafan.xyz`) is a third-party mirror and may go offline.\n"
                    "Try the normal **PW without Purchase** button instead (requires your token)."
                )
                return

            text = ""
            for i, batch in enumerate(data[:20]):
                name = batch.get("batch_name") or batch.get("name") or str(batch)
                text += f"{i+1}. ```\n{name}```\n"

            await editable.edit(f"**Found {len(data)} batches:\n\n{text}\nSend index number:**")

            try:
                inp2 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                idx = int(inp2.text.strip()) - 1
                await inp2.delete(True)
            except (ListenerTimeout, ValueError):
                await editable.edit("**Timeout or invalid input**"); return

            if not (0 <= idx < len(data)):
                await editable.edit("**Invalid index**"); return

            batch = data[idx]
            batch_id = batch.get("batch_id") or batch.get("_id") or batch.get("id")
            batch_name = batch.get("batch_name") or batch.get("name") or "Unknown"

            if not batch_id:
                await editable.edit("**❌ Could not get batch ID from API response**"); return

            await editable.edit(f"**📚 Batch: {batch_name}\nFetching subjects...**")

            # Get subjects
            subjects_data = await suarkafan_get(session, "getAllSubjects.php", {"batch_id": batch_id})
            if not subjects_data or not isinstance(subjects_data, list):
                await editable.edit("**❌ No subjects found**"); return

            text = ""
            for i, s in enumerate(subjects_data):
                sname = s.get("subject_name") or s.get("name") or s.get("subject") or str(i+1)
                text += f"{i+1}. ```\n{sname}```\n"

            await editable.edit(f"**Subjects:\n\n{text}\nSend index (or `all` for all subjects):**")

            try:
                inp3 = await bot.listen(chat_id=m.chat.id, filters=filters.user(user_id), timeout=120)
                sinp = inp3.text.strip()
                await inp3.delete(True)
            except ListenerTimeout:
                await editable.edit("**Timeout**"); return

            if sinp.lower() == "all":
                selected_subjects = subjects_data
            else:
                try:
                    sidx = int(sinp) - 1
                    if not (0 <= sidx < len(subjects_data)):
                        await editable.edit("**Invalid index**"); return
                    selected_subjects = [subjects_data[sidx]]
                except ValueError:
                    await editable.edit("**Invalid input**"); return

            await editable.edit(f"**⏳ Extracting {batch_name}...\nThis may take a minute.**")

            import time as _time
            start = _time.time()
            clean_name = batch_name.replace("/", "-").replace("|", "-")[:80]
            clean_file = f"{user_id}_{clean_name}"

            all_lines = []
            skip_topics = ["new feature update", "new update", "important update", "new study page"]

            for subject in selected_subjects:
                subject_id = subject.get("subject_id") or subject.get("_id") or subject.get("id")
                subject_name = subject.get("subject_name") or subject.get("name") or subject.get("subject") or "Unknown"

                if not subject_id:
                    continue

                # Get chapters
                chapters_data = await suarkafan_get(session, "getAllChapters.php",
                    {"batch_id": batch_id, "subject_id": subject_id})
                chapters = chapters_data if isinstance(chapters_data, list) else []

                # Get lectures (videos)
                lectures_data = await suarkafan_get(session, "getAllLectures.php",
                    {"batch_id": batch_id, "subject_id": subject_id})
                lectures = lectures_data if isinstance(lectures_data, list) else []

                for lec in lectures:
                    topic = (lec.get("lecture_topic") or lec.get("topic") or "Unknown").strip()
                    if any(k in topic.lower() for k in skip_topics):
                        continue
                    url = lec.get("lecture_videoUrl") or lec.get("videoUrl") or lec.get("url") or ""
                    if url:
                        all_lines.append(f"{topic}:{url}")

                # Get notes
                notes_data = await suarkafan_get(session, "getAllNotes.php",
                    {"batch_id": batch_id, "subject_id": subject_id})
                notes = notes_data if isinstance(notes_data, list) else []

                for note in notes:
                    topic = (note.get("topic") or note.get("name") or "Note").strip()
                    url = note.get("url") or note.get("baseUrl", "") + note.get("key", "")
                    if url:
                        all_lines.append(f"{topic}:{url}")

                # Get DPPs
                dpps_data = await suarkafan_get(session, "getAllDpps.php",
                    {"batch_id": batch_id, "subject_id": subject_id})
                dpps = dpps_data if isinstance(dpps_data, list) else []

                for dpp in dpps:
                    topic = (dpp.get("topic") or dpp.get("name") or "DPP").strip()
                    url = dpp.get("url") or dpp.get("baseUrl", "") + dpp.get("key", "")
                    if url:
                        all_lines.append(f"{topic}:{url}")

            if not all_lines:
                await editable.edit(
                    "**❌ No content found.**\n\n"
                    "The free API may have different field names or this batch isn't in its database yet.\n"
                    "Share the raw API response with the bot admin to fix field mapping."
                )
                return

            elapsed = _time.time() - start
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)
            fmt_time = f"{mins}m {secs}s" if mins else f"{secs}s"

            # Write txt
            with open(f"{clean_file}.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(all_lines))

            await editable.delete(True)
            caption = (
                f"**🆓 Free Extract**\n"
                f"**Batch:** ```\n{batch_name}```"
                f"**Items:** ```\n{len(all_lines)}```"
                f"**Time:** ```\n{fmt_time}```"
            )

            with open(f"{clean_file}.txt", "rb") as f:
                await m.reply_document(document=f, caption=caption, file_name=f"{clean_name}.txt")
            os.remove(f"{clean_file}.txt")

        except Exception as e:
            logging.exception(f"pwfree error: {e}")
            try:
                await editable.edit(f"**Error: {e}**")
            except Exception:
                pass
        finally:
            await session.close()
            await CONNECTOR.close()

if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    bot.run()
