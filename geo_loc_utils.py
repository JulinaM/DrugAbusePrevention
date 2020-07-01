# DrugVis
# By: Han Hu (hh255@njit.edu; https://github.com/hu7han73)
# License: BSD

import re

from twokenize import normalizeTextForTagger, tokenize


def get_state_fips_list():
    state_fips_str = '''Alabama 	AL 	01
  Alaska 	AK 	02
  Arizona 	AZ 	04
  Arkansas 	AR 	05
  California 	CA 	06
  Colorado 	CO 	08
  Connecticut 	CT 	09
  Delaware 	DE 	10
  District of Columbia  DC  11
  Florida 	FL 	12
  Georgia 	GA 	13
  Hawaii 	HI 	15
  Idaho 	ID 	16
  Illinois 	IL 	17
  Indiana 	IN 	18
  Iowa 	IA 	19
  Kansas 	KS 	20
  Kentucky 	KY 	21
  Louisiana 	LA 	22
  Maine 	ME 	23
  Maryland 	MD 	24
  Massachusetts 	MA 	25
  Michigan 	MI 	26
  Minnesota 	MN 	27
  Mississippi 	MS 	28
  Missouri 	MO 	29
  Montana 	MT 	30
  Nebraska 	NE 	31
  Nevada 	NV 	32
  New Hampshire 	NH 	33
  New Jersey 	NJ 	34
  New Mexico 	NM 	35
  New York 	NY 	36
  North Carolina 	NC 	37
  North Dakota 	ND 	38
  Ohio 	OH 	39
  Oklahoma 	OK 	40
  Oregon 	OR 	41
  Pennsylvania 	PA 	42
  Rhode Island 	RI 	44
  South Carolina 	SC 	45
  South Dakota 	SD 	46
  Tennessee 	TN 	47
  Texas 	TX 	48
  Utah 	UT 	49
  Vermont 	VT 	50
  Virginia 	VA 	51
  Washington 	WA 	53
  West Virginia 	WV 	54
  Wisconsin 	WI 	55
  Wyoming 	WY 	56
  American Samoa 	AS 	60
  Guam 	GU 	66
  Northern Mariana Islands 	MP 	69
  Puerto Rico 	PR 	72
  Virgin Islands 	VI 	78'''
    states_str = state_fips_str.split('\n')
    state_fips_list = []
    for state_str in states_str:
        s = state_str.split()
        # print(s)
        state = ' '.join(s[:-2])
        # print(state)
        abbr = s[-2]
        fips = s[-1]
        if state not in ['Hawaii', 'Alaska', 'American Samoa', 'Guam', 'Northern Mariana Islands', 'Puerto Rico',
                         'Virgin Islands']:
            state_fips_list.append(fips)
    return state_fips_list


def min_max(s, min, max):
    return s.sub(min).div(max - min)


def get_states():
    state_fips_str = '''Alabama 	AL 	01
  Alaska 	AK 	02
  Arizona 	AZ 	04
  Arkansas 	AR 	05
  California 	CA 	06
  Colorado 	CO 	08
  Connecticut 	CT 	09
  Delaware 	DE 	10
  District of Columbia  DC  11
  Florida 	FL 	12
  Georgia 	GA 	13
  Hawaii 	HI 	15
  Idaho 	ID 	16
  Illinois 	IL 	17
  Indiana 	IN 	18
  Iowa 	IA 	19
  Kansas 	KS 	20
  Kentucky 	KY 	21
  Louisiana 	LA 	22
  Maine 	ME 	23
  Maryland 	MD 	24
  Massachusetts 	MA 	25
  Michigan 	MI 	26
  Minnesota 	MN 	27
  Mississippi 	MS 	28
  Missouri 	MO 	29
  Montana 	MT 	30
  Nebraska 	NE 	31
  Nevada 	NV 	32
  New Hampshire 	NH 	33
  New Jersey 	NJ 	34
  New Mexico 	NM 	35
  New York 	NY 	36
  North Carolina 	NC 	37
  North Dakota 	ND 	38
  Ohio 	OH 	39
  Oklahoma 	OK 	40
  Oregon 	OR 	41
  Pennsylvania 	PA 	42
  Rhode Island 	RI 	44
  South Carolina 	SC 	45
  South Dakota 	SD 	46
  Tennessee 	TN 	47
  Texas 	TX 	48
  Utah 	UT 	49
  Vermont 	VT 	50
  Virginia 	VA 	51
  Washington 	WA 	53
  West Virginia 	WV 	54
  Wisconsin 	WI 	55
  Wyoming 	WY 	56
  American Samoa 	AS 	60
  Guam 	GU 	66
  Northern Mariana Islands 	MP 	69
  Puerto Rico 	PR 	72
  Virgin Islands 	VI 	78'''
    state_list = []
    for state_str in state_fips_str.split('\n'):
        s = state_str.split()
        # print(s)
        state = ' '.join(s[:-2])
        # print(state)
        if state not in ['Hawaii', 'Alaska', 'American Samoa', 'Guam', 'Northern Mariana Islands', 'Puerto Rico',
                         'Virgin Islands']:
            state_list.append(state)
    for s in state_list:
        print("\"" + s + '\",')
    print(len(state_list))


emoji_pat = '[\U0001F300-\U0001F64F\U0001F680-\U0001F6FF\U00002600-\U000027BF\U0001F910-\U0001F96B\U0001F980-\U0001F9E0\u2600-\u26FF\u2700-\u27BF]'
shrink_whitespace_reg = re.compile(r'\s{2,}')
reg = re.compile(r"({})|[^'@#% 0-9a-zA-Z]".format(emoji_pat))


def replace_sp_tokens(text):
    re_mention = re.compile(r"\B@\w+\b")
    re_url = re.compile(
        r"(http|ftp|https)://([a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF_-]+(?:(?:\.[a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF_-]+)+))([a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF.,@?^=%&:/~+#-]*[a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF@?^=%&/~+#-])?")
    text = re.sub(re_mention, '%USER_MENTION%', text)
    text = re.sub(re_url, '%URL%', text)
    return text


def clean(text):
    result = reg.sub(lambda x: ' {} '.format(x.group(1)) if x.group(1) else ' ', text)
    return shrink_whitespace_reg.sub(' ', result)


def get_tokens(text):
    return tokenize(clean(replace_sp_tokens(normalizeTextForTagger(text))))


def read_drug_category():
    behav_list = ['buzz', 'abuse', 'amp', 'bake', 'high as kite', 'doped up', 'high',
                  'jacked up', 'jucing', 'strung out', 'bust', 'clean', 'come down',
                  'comedown', 'crack den', 'drop', 'fix', 'hit', 'huff', 'inhale',
                  'inject', 'juice', 'lace', 'mainline', 'overdose', 'peddle',
                  'permafry', 'pop', 'push', 're up', 'rush', 'score',
                  'sell', 'shoot', 'shoot up', 'smkoe', 'smuggle', 'sniff', 'snort',
                  'spike', 'spliff', 'stone', 'strung out', 'supply',
                  'take', 'take hit', 'toke', 'trip', 'tweak',
                  'use', 'waste']
    symptom_list = ['anxiety disorder', 'attention deficit hyperactivity disorder',
                    'ADHD', 'bipolar disorder', 'chronic pain', 'major depressive disorder',
                    'obstructive lung disease', 'sleep disorder', 'insomnia',
                    'agitation', 'anxiety', 'blackout', 'coma', 'depression',
                    'diarrhea', 'withdrawal symptom', 'difficulty concentrating',
                    'disturbed sleep', 'flu like symptom', 'headache', 'heart palpitations',
                    'hypertension', 'irregular heart rate', 'muscle pain', 'muscle stiffness',
                    'panic attacks', 'restlessness', 'seizures', 'short term memory loss',
                    'sweating', 'tremors', 'fast heart rate', 'fever', 'hallucinations',
                    'heartburn', 'infections', 'inflammation', 'irritability', 'lethargy',
                    'mood swing', 'myalgia', 'nausea', 'paranoia', 'pupil dilation', 'intoxication']
    drug_category = {}
    with open('./illegal_drug_names.csv', 'r') as inf:
        lines = inf.readlines()
    for l in lines:
        d_list = l.lower().split(',')
        drug_category[d_list[0]] = [d.strip() for d in d_list if len(d) > 2]
    drug_category['behavior'] = behav_list
    drug_category['symptom'] = symptom_list
    return drug_category


def read_stopwords():
    with open('./custom_stopwords2.txt', 'r') as inf:
        lines = inf.readlines()
    stopwords = [w.strip() for w in lines]
    return stopwords
