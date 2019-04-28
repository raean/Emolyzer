# The Emolyzer is a Python scripts that generates a .csv data file of songs litened to by users of Last.FM where the users are extracted from the Two Million LastFM User Profiles (TMLUP) dataset, their listening song history has been extracted from Last.FM API, the links of the song lyrics are extracted by the Genius API, the lyrics of the songs are exctracted by BeautifulSoup and HTTP requests, and the analysis is done using the NRC lexicon.
# The format of the .csv file outputted has columns: ID, Artist, Track, Date, Link, Lyrics, Negative, Neutral, Positive, Anger, Disgust, Fear, Joy, Sadness, Surprise, Anticipation, Trust
# Input: num_songs (number of songs you want per user / Maximum: 1000), num_user (number of users you want to analyze / Maximum: 1,840,647), filename (the name of the .csv file you want the results saved to).

# Setting up imports:
import sys, csv, os, re, math, operator, time, pylast, pandas, time, itertools, urllib, json, multiprocessing, requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from itertools import product


# Setting up API keys for Last.FM and Genius:
GENIUS_API_TOKEN = 'PLACE YOUR KEY HERE'
LAST_FM_API_KEY = 'PLACE YOUR KEY HERE'
LAST_FM_SECRET_KEY = 'PLACE YOUR KEY HERE'
LAST_FM_USERNAME = 'PLACE YOUR USERNAME HERE'
LAST_FM_PW = pylast.md5('PLACE YOUR PASSWORD HERE')
PATH_OF_TMLUP = 'Two_Million_LastFM_User_Profiles.csv'
last_fm_network = pylast.LastFMNetwork(api_key=LAST_FM_API_KEY, api_secret=LAST_FM_SECRET_KEY, username=LAST_FM_USERNAME, password_hash=LAST_FM_PW)

# This function retrieves the history of users from the TMLUP dataset. Arguments(start_user_range: the starting index of where to get users, end_user_range: the ending index of where to get users, num_of_songs_per_user: the number of songs per user. Returns: a list of songs from a range of users.
def get_history(start_user_range, end_user_range, num_of_songs_per_user):
    csvdata = csv.reader(open(PATH_OF_TMLUP, 'r'))
    data = itertools.islice(csvdata, start_user_range, end_user_range+1)
    song_list = []
    for username in data:
        # We surround this in a try-except in the case the user no longer exists.
        try:
            username_string = username[1]
            user_object = last_fm_network.get_user(username_string)
            listening_history = user_object.get_recent_tracks(limit=num_of_songs_per_user, cacheable=True) # bottleneck is  here.
            # We append all the songs from the listening_histor of a user into a list of songs.
            for song in listening_history:
                song_list.append([song[0].artist.get_name(),song[0].title,song[2]])
        except Exception as e:
            print("[!] Error: " + str(e))
    return song_list

# This function assembles all the results from processess and puts them into one result (whilst removing duplicate songs). Arguments(process_result: a list of each result from each process). Returns: a list of unique songs found amongst all the processess.
def compile_process_results(process_result):
    songs = []
    temp_songs = []
    for process_item in process_result:
        for item in process_item:
            for sub_item in item:
                # We only compare the name and arist of the song, not the date.
                # Two songs might have the same artist and track, but not the same date.
                if sub_item[0:2] not in temp_songs:
                    songs.append(sub_item)
                    temp_songs.append(sub_item[0:2])
    return songs

# This functions get the lyric≠≠≠≠≠s of each songs in a list of songs, if it fails, it does not add it back to the list of songs and is removed. This function calls two helper functions search() & find_lyrics_div() which use the Genius API and BeautifulSoup library to query and crawl songs from the web. Arguments(songs: a list of songs which lyrics are wanted). Returns: a list of the same songs with the lyrics & url appended to the if the attempt was successful.
def get_lyrics(songs):
    # Indexing of songs: [artist, track, date]
    songs_with_lyrics = []
    for song in songs:
        artist = song[0]
        track = song[1]
        # If the search failed, we label the result[1] as "Failed." in order to not add it back to the list.
        try:
            result = search(artist, track)
        except:
            result[1] = "Failed."
        if result[1] != "Failed.":
            songs_with_lyrics.append([artist, track, song[2], result[0], result[1]])
    return songs_with_lyrics

def find_lyrics_div(url):
    try:
        page = requests.get(url+"?access_token="+GENIUS_API_TOKEN).text
        soup = BeautifulSoup(page, 'lxml')
        body = soup.body.find_all('div')[20].contents[1].find_all('div')[0].contents[3].find_all('div')[0]
        body2 = body.text.replace('\n', '. ').replace('\r', '. ').split(' ')
        lyrics = ""
        for item in body2:
            lyrics = lyrics + " " + item.lower()
        return lyrics
    except:
        return "Failed."

# This function creates a querystring with the artist name and song as the search terms and finds the link at which the songs exists. It then requests the webpage and calls find_lyrics_div() to get the lyrics <div> element inner HTML text. Arguments(artist: the name of the artist, track: the name of the song). Returns: the lyrics of the song and the url of where it exists. NOTE: This function was inspired by Github user jasonqng and his source code can be found at: https://github.com/jasonqng/genius-lyrics-search
def search(artist, track):
    lyrics = ""
    page = 1
    search_term = artist + " " + track
    querystring = "http://api.genius.com/search?q=" + urllib.parse.quote(search_term) + "&page=" + str(page)

    while True:
        try:
            response = urllib.request.urlopen(querystring+"&access_token="+GENIUS_API_TOKEN)
            raw = response.read()
        except Exception as e:
            print("[!] Error: " + str(e))
            continue
        break

    json_obj = json.loads(raw)
    body = json_obj["response"]["hits"]
    lyrics = "Failed."
    url = ""

    num_hits = len(body)
    if num_hits==0:
        if page==1:
            print("[!] Error: No results for: " + search_term)
    else:
        url = body[0]["result"]["url"]
        primaryartist_name = body[0]["result"]["primary_artist"]["name"]
        if (primaryartist_name.lower() == artist.lower()):
            lyrics = find_lyrics_div(url)
    return [url, lyrics]

# This function performs a Lexicon-based emotional analysis on song lyrics using the National Research Council Canada (NRC) Emotion Lexicon. Arguments(lyrics: the lyrics of the song to be emotionally analyzed). Returns: the scoring of each emotional category in this lexicon.
def emotional_analysis(lyrics):
    nrc = {}
    with open("nrc.txt") as n:
    	for line in n:
    		line = line.rstrip()
    		sp = line.split("\t")
    		if sp[0] in nrc:
    			nrc[sp[0]].append(sp[2])
    		else:
    			nrc[sp[0]] = [sp[2]]

    sum_nrc_ag = 0
    sum_nrc_dg = 0
    sum_nrc_fe = 0
    sum_nrc_jo = 0
    sum_nrc_sa = 0
    sum_nrc_sp = 0
    sum_nrc_an = 0
    sum_nrc_tr = 0
    for line in lyrics.split('.'):
        nrc_ag = 0
        nrc_dg = 0
        nrc_fe = 0
        nrc_jo = 0
        nrc_sa = 0
        nrc_sp = 0
        nrc_an = 0
        nrc_tr = 0
        tokenized_text = (line.lower().split(" "))
        for word in tokenized_text:
            if word in nrc:
                ag = int(nrc[word][0]) # anger
                dg = int(nrc[word][2]) # disgust
                fe = int(nrc[word][3]) # fear
                jo = int(nrc[word][4]) # joy/happiness
                sa = int(nrc[word][7]) # sadness
                sp = int(nrc[word][8]) # surprise
                an = int(nrc[word][1]) # anticipation
                tr = int(nrc[word][9]) # trust
                nrc_ag = nrc_ag + ag # anger
                nrc_dg = nrc_dg + dg # disgust
                nrc_fe = nrc_fe + fe # fear
                nrc_jo = nrc_jo + jo # joy/happiness
                nrc_sa = nrc_sa + sa # sadness
                nrc_sp = nrc_sp + sp # surprise
                nrc_an = nrc_an + an # anticipation
                nrc_tr = nrc_tr + tr # trust
        sum_nrc_ag = sum_nrc_ag + nrc_ag
        sum_nrc_dg = sum_nrc_dg + nrc_dg
        sum_nrc_fe = sum_nrc_fe + nrc_fe
        sum_nrc_jo = sum_nrc_jo + nrc_jo
        sum_nrc_sa = sum_nrc_sa + nrc_sa
        sum_nrc_sp = sum_nrc_sp + nrc_sp
        sum_nrc_an = sum_nrc_an + nrc_an
        sum_nrc_tr = sum_nrc_tr + nrc_tr
        dict_nrc = {"[anger]":nrc_ag, "[disgust]":nrc_dg, "[fear]":nrc_fe, "[joy]":nrc_jo, "[sadness]":nrc_sa, "[surprise]":nrc_sp, "[anticipation]":nrc_an, "[trust]":nrc_tr}
        sum = 1 + sum_nrc_ag + sum_nrc_dg + sum_nrc_fe + sum_nrc_jo  + sum_nrc_sa + sum_nrc_sp + sum_nrc_an + sum_nrc_tr
    output = [(sum_nrc_ag/sum),(sum_nrc_dg/sum),(sum_nrc_fe/sum),(sum_nrc_jo/sum),(sum_nrc_sa/sum), (sum_nrc_sp/sum),(sum_nrc_an/sum),(sum_nrc_tr/sum)]
    return output

# This function performs a Lexicon-based sentiment analysis on song lyrics using the National Research Council Canada (NRC) Emotion Lexicon. Arguments(lyrics: the lyrics of the song to be sentimentally analyzed). Returns: the scoring of each sentiment category in this lexicon.
def sentiment_analysis(lyrics):
    nrc = {}
    with open("nrc.txt") as n:
    	for line in n:
    		line = line.rstrip()
    		sp = line.split("\t")
    		if sp[0] in nrc:
    			nrc[sp[0]].append(sp[2])
    		else:
    			nrc[sp[0]] = [sp[2]]



    sum_neg = 0
    sum_pos = 0
    sum_nrl = 0

    for line in lyrics.split('.'):
        neg = 0
        pos = 0
        nrl = 0
        tokenized_text = (line.lower().split(" "))
        for word in tokenized_text:
            if word in nrc:
                n_rating = int(nrc[word][5])
                p_rating = int(nrc[word][6])
                neg = neg + n_rating # negative
                pos = pos + p_rating # positive
                if n_rating == 0 and p_rating == 0:
                    nrl = nrl + 1
        sum_neg = sum_neg + neg
        sum_pos = sum_pos + pos
        sum_nrl = sum_nrl + nrl
        dict_nrc = {"[+]":pos, "[-]":neg, "[=]": nrl}
    sum = sum_neg + sum_pos + sum_nrl + 1
    return ([sum_neg/sum,sum_nrl/sum,sum_pos/sum])

# This function gathers all the information performed by this script and stores the data in a .csv file. Arguments(filename: the name of the .csv file intended, songs: the list of songs with their artist, name, lyrics, url, sentiment scores and emotional scores).
def create_csv_file(filename, songs):
    with open(filename+'.csv', 'w', newline='') as csvfile:
        fieldnames = ['id','artist','track','date','url','lyrics','negative','neutral','positive','anger','disgust','fear','joy','sadness','surprise','anticipation','trust']
        spamwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
        spamwriter.writeheader()
        count = 0
        for song in songs:
            for value in song:
                try:
                    spamwriter.writerow({'id':count,'artist':value[0],'track':value[1],'date':value[2], 'url':value[3],'lyrics':value[4],'negative':value[5],'neutral':value[6],'positive':value[7],'anger':value[8],'disgust':value[9],'fear':value[10],'joy':value[11],'sadness':value[12],'surprise':value[13],'anticipation':value[14],'trust':value[15]})
                    count = count + 1
                except Exception as e:
                    print("[!] Error: #" + str(count) + ": Failed. Message: " + str(e))

# This function simply calls the emotional analysis and sentiment analysis functions and links them back to the songs. Argument(songs_with_lyrics: the lyrics of the songs)
def analyze_songs(songs_with_lyrics):
    # Indexing of songs: [artist, track, date, url, lyrics]
    songs_with_analysis = []
    for song in songs_with_lyrics:
        emotional_analysis_score = emotional_analysis(song[4])
        sentiment_analysis_score = sentiment_analysis(song[4])
        songs_with_analysis.append([song[0], song[1], song[2], song[3], song[4],
        sentiment_analysis_score[0], sentiment_analysis_score[1], sentiment_analysis_score[2],
        emotional_analysis_score[0], emotional_analysis_score[1], emotional_analysis_score[2], emotional_analysis_score[3], emotional_analysis_score[4], emotional_analysis_score[5], emotional_analysis_score[6], emotional_analysis_score[7]])
    return songs_with_analysis

# This function compiles the analysis results from each process. Arguments(songs: a list of songs). Returns: the arguments ready to be split.
def compile_analysis_argument_input(songs):
    arguments = []
    for process in songs:
        for process_result in process:
            for song in process_result:
                arguments.append(song)
    return arguments

# This function simply calls all the required actions to retrieve the completed dataset. Arguments(num_songs: number of songs you want per user - Maximum: 1000, num_user: number of users you want to analyze - Maximum: 1,840,647, filename: the name of the .csv file you want the results saved to)
def run(num_songs, num_user, filename):
    start = time.time()
    # We split the number of users we want to get history from mongst 6 processes and put them in a list as follows:
    process_result = []
    arguments = [[0, int(1*(num_user/6)-1), num_songs], [int(1*(num_user/6)), int(2*(num_user/6)-1), num_songs], [int(2*(num_user/6)), int(3*(num_user/6)-1), num_songs], [int(3*(num_user/6)), int(4*(num_user/6)-1), num_songs], [int(4*(num_user/6)), int(5*(num_user/6)-1), num_songs], [int(6*(num_user/6)), int(6*(num_user/6)-1), num_songs]]

    # We use the multiprocessing tool to exercise throughput and split the work to get history of users.
    with multiprocessing.Pool(processes=6) as pool:
        process_result.append(pool.starmap(get_history, arguments))
    end = time.time()
    print("The execution of get_history() took: " + str(end-start) + " seconds.")

    # We call this function to compile all the results of the processess to put them into one cohesive list.
    start = time.time()
    songs = compile_process_results(process_result)
    end = time.time()
    print("The execution of compile_process_results() took: " + str(end-start) + " seconds.")

    # Similarly to getting the history, we split the work of getting the lyrics amongst 24 processes:
    start = time.time()
    lyrics_arguments = [[songs[0:int((len(songs)/24))-1]],
    [songs[int((len(songs)/24)):2*int((len(songs)/24))-1]],
    [songs[2*int((len(songs)/24)):3*int((len(songs)/24))-1]],
    [songs[3*int((len(songs)/24)):4*int((len(songs)/24))-1]],
    [songs[4*int((len(songs)/24)):5*int((len(songs)/24))-1]],
    [songs[5*int((len(songs)/24)):6*int((len(songs)/24))-1]],
    [songs[6*int((len(songs)/24)):7*int((len(songs)/24))-1]],
    [songs[7*int((len(songs)/24)):8*int((len(songs)/24))-1]],
    [songs[8*int((len(songs)/24)):9*int((len(songs)/24))-1]],
    [songs[9*int((len(songs)/24)):10*int((len(songs)/24))-1]],
    [songs[10*int((len(songs)/24)):11*int((len(songs)/24))-1]],
    [songs[11*int((len(songs)/24)):12*int((len(songs)/24))-1]],
    [songs[12*int((len(songs)/24)):13*int((len(songs)/24))-1]],
    [songs[13*int((len(songs)/24)):14*int((len(songs)/24))-1]],
    [songs[14*int((len(songs)/24)):15*int((len(songs)/24))-1]],
    [songs[15*int((len(songs)/24)):16*int((len(songs)/24))-1]],
    [songs[16*int((len(songs)/24)):17*int((len(songs)/24))-1]],
    [songs[17*int((len(songs)/24)):18*int((len(songs)/24))-1]],
    [songs[18*int((len(songs)/24)):19*int((len(songs)/24))-1]],
    [songs[19*int((len(songs)/24)):20*int((len(songs)/24))-1]],
    [songs[20*int((len(songs)/24)):21*int((len(songs)/24))-1]],
    [songs[21*int((len(songs)/24)):22*int((len(songs)/24))-1]],
    [songs[22*int((len(songs)/24)):23*int((len(songs)/24))-1]],
    [songs[23*int((len(songs)/24)):24*int((len(songs)/24))-1]]]
    songs_with_lyrics = []
    with multiprocessing.Pool(processes=24) as pool:
        songs_with_lyrics.append(pool.starmap(get_lyrics, lyrics_arguments))
    end = time.time()
    print("The execution of get_lyrics() took: " + str(end-start) + " seconds.")

    # We then once against compile the results from the processes and split the work to analyze the songs amongst 6 processes.
    start = time.time()
    temp = compile_analysis_argument_input(songs_with_lyrics)
    print("Number of songs with lyrics found: " + str(len(temp)))
    analysis_arguments = [[temp[0:int(len(temp)/6)-1]],
    [temp[int(len(temp)/6):2*int(len(temp)/6)-1]],
    [temp[2*int(len(temp)/6):3*int(len(temp)/6)-1]],
    [temp[3*int(len(temp)/6):4*int(len(temp)/6)-1]],
    [temp[4*int(len(temp)/6):5*int(len(temp)/6)-1]],
    [temp[5*int(len(temp)/6):6*int(len(temp)/6)-1]]]
    with multiprocessing.Pool(processes=6) as pool:
        songs_with_analysis = pool.starmap(analyze_songs, analysis_arguments)
    end = time.time()
    print("The execution of analyze_songs() took: " + str(end-start) + " seconds.")

    # We call the following funciton to create the .csv file to embed all the information we've gathered into a .csv file.
    start = time.time()
    create_csv_file(filename, songs_with_analysis)
    end = time.time()
    print("The execution of create_csv_file() took: " + str(end-start) + " seconds.")

    print("Done!")

def main():
    # This is where you put your input parameters:
    num_songs = sys.argv[1]
    num_user = sys.argv[2]
    filename = sys.argv[3]

    start = time.time()
    run(num_songs, num_user, filename) # We run the main components that call all functions here.
    end = time.time()
    print("Enter time of all operations: " + str(end-start) + " seconds.")

if __name__ == '__main__':
    main()
