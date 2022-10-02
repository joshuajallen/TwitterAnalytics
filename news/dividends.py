import spacy
from spacy import displacy
import pandas as pd 

strep = pd.read_csv("string_replace.csv")
str_replace = [(x, y) for x, y in zip(list(strep.string), list(strep.replacement))]

nlp = spacy.load("en_core_web_sm")

df = pd.read_csv("all_news.txt", sep = "\t")

#df_divi = df[['dividend' in x.lower() or ' divi ' in x.lower() for x in df['title']]].copy()

flatten = lambda l: [item for sublist in l for item in sublist]

def get_token(x, verb):
    doc = nlp(x)
    token = next((token for token in doc if token.text == verb), None)
    return(token)

def get_subject(x, verb):
    token = get_token(x, verb)
    if token == None:
        return([])
    subject = next((child for child in token.children if child.dep_ == 'nsubj'), None)
    subject = get_compound(subject) + [subject]
    return(subject)

def get_compound(token):
    if token == None:
        return([])
    res = [x for x in token.children if x.dep_ == "compound" or x.dep_ == "amod"]
    return(res)

def get_object(x, verb):
    token = get_token(x, verb)
    if token == None:
        return(None)
    children = list(token.children)
    obj = None
    while obj == None and len(children) > 0:
        obj = next((child for child in children if 'obj' in child.dep_), None)
        children = flatten([list(x.children) for x in children])
    obj = get_compound(obj) + [obj]
    return(obj)



def get_verb(x):
    x = x.lower()
    for k, v in str_replace:
        x = x.replace(k, v)

    doc = nlp(x)

    divi_tokens = [token for token in doc if token.text == 'dividend' or token.text == 'dividends' or token.text == 'divi' or token.text == 'divis']
    if len(divi_tokens) == 0:
        return("")
    divi_token = divi_tokens[0]

    head = divi_token.head
    while head.pos_ != "VERB" and head.dep_ != "ROOT":
        head = head.head
    if head.pos_ == "VERB":
        return(head.text)
    
    return("")  

#df_divi["verb"] = [get_verb(x) for x in list(df_divi["title"])]

def render(text):
    doc = nlp(text)
    ofile = open("test.html", "w")
    ofile.writelines(displacy.render(doc, style = 'dep'))
    ofile.close()