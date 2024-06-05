import re
from natasha import (
    Segmenter,
    MorphVocab,
    AddrExtractor
)

segmenter = Segmenter()
morph_vocab = MorphVocab()
addr_extractor = AddrExtractor(morph_vocab)


def extract_index(string, errors=False):  # 100% works !!!
    '''
    Извлекает индекс из строки
    Вход: строка
    Выход: адрес без индекса, индекс
    '''
    index = re.findall(r'[^| |,][\d]{5}[ |$|, ]', string)
    if len(index) > 1 and errors:
        print("Два индекса в строке \"%s\" ?" % string)

    if index:
        index = index[0]
        string = string.replace(index, '').strip()
        index = index.replace(',', '')
    else:
        index = None
    return string, index


def process_address(string):
    string, index = extract_index(string)
    # Превращает "2c3" в "2 c 3"  и  "2-3" в "2 - 3"
    string = re.sub(r"[\d]+|[\W]+", ' \g<0> ', string)
    # то же самое, только с запятыми
    string = string.replace(',', ', ')
    string = re.sub(r'\,|\.|\-|\'|\"|\(|\)', '', string)
    string = re.sub(r' +', ' ', string)
    matches = addr_extractor(string)
    facts = [i.fact.as_json for i in matches]
    tokens = []
    flat = None
    for fact in facts:
        name, type = fact['value'], fact['type'] if 'type' in fact else None
        if name not in tokens and type not in ('страна', 'квартира'):
            tokens.append(name.lower())
        if 'type' in fact and fact['type'] == 'квартира':
            flat = name
    return {'postal_code': index, 'flat': flat, 'raw_address': string, 'search_string': ' '.join(tokens)}

