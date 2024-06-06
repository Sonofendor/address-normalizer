import re
from natasha import (
    Segmenter,
    MorphVocab,
    AddrExtractor
)
import itertools
import operator

segmenter = Segmenter()
morph_vocab = MorphVocab()
addr_extractor = AddrExtractor(morph_vocab)


def inverdic(dic):
    resdic = {}
    for key, value in dic.items():
        for index in value:
            if type(value) == set or type(value) == list:
                if index in resdic.keys():
                    if isinstance(resdic[index], set):
                        resdic[index].add(key)
                    elif isinstance(resdic[index], list):
                        resdic[index].append(key)
                    else:
                        resdic[index] = [resdic[index]]
                        resdic[index].append(key)
                else:
                    resdic[index] = key
            elif type(value) == dict:
                resdic.update(inverdic(value))
    return resdic


sep_house_signs = {
    'дом': {'д', 'дом'},
    'домовладение': {'домовладение', 'двл', 'двлд'},
    'владение': {'владение', 'вл', 'влд'},
    'корпус': {'к', 'корп', 'копр', 'кор', 'корпус'},
    'строение': {'с', 'стр', 'строен', 'строение'},
    'квартира': {'кв', 'квартира'},
    'помещение': {'пом', 'помещение'},
    'комната': {"ком", 'комн', 'комната'},
    'кабинет': {"кабинет", "каб", "к-т", "каб-т"},
    'офис': {'оф', 'офис'},
    'литера': set("абвежз"),
    'прочее': {'литер', 'литера', 'лит'},
    'дробь': {'/', '-'}
}

house_signs_inv = inverdic(sep_house_signs)


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


def tokenize(string, comma=False):
    '''
    Токенизатор. Раздвигает слипшиеся буквы и цифры вроде 2с3 или корп1
    Вход: строка
    Выход: массив из слов (токены)
    '''
    string = string.lower()
    if not comma:
        return re.findall(r'[\d]+|[\w]+', string)
    if comma:
        return re.findall(r'[\d]+|[\w]+|\,', string)


def extract_house_tokens(tokens):
    '''
    находит последовательность номеров дома/корпуса/строения среди токенов и возвращает их
    '''
    a = lambda x: "число" if x.isdigit() and len(x) < 6 else "препинания" if x == ',' else "не распознано"
    types = [house_signs_inv.get(x, a(x)) for x in tokens]
    types_bin = [0 if x == 'не распознано' else 1 for x in types]
    array = list((list(y) for (x, y) in itertools.groupby((enumerate(types_bin)), operator.itemgetter(1)) if x == 1))
    if len(array) == 0:
        return [], []
    longest_seq = max(reversed(array), key=len)
    return [tokens[i] for (i, _) in longest_seq], [types[i] for (i, _) in longest_seq]


def tokens_to_string(tokens, string):
    '''
    Ищет токены в строке и возвращает их позицию начала
    '''
    pattern = r".?.?".join(tokens)
    found = re.search(pattern, string.lower())
    if found == None:
        split = len(string)
    else:
        split = found.start()
    return split


def multiple_replace(dict, text, compiled=False):
    '''
    Преобразует словарь замен (dict) в паттерн замен для регулярок и тут же применяет его
    '''
    if not compiled:
        regex = re.compile(r"\b(%s)\b" % "|".join(map(re.escape, dict.keys())))
    else:
        regex = compiled

    # For each match, look-up corresponding value in dictionary
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text)


def process_address(string):
    string, index = extract_index(string)
    # Превращает "2c3" в "2 c 3"  и  "2-3" в "2 - 3"
    string = re.sub(r"[\d]+|[\W]+", ' \g<0> ', string)
    # то же самое, только с запятыми
    string.replace('ё', 'е')
    string = string.replace(',', ', ')
    string = re.sub(r'\,|\.|\-|\'|\"|\(|\)|№', '', string)
    string = re.sub(r' +', ' ', string)
    house_tokens, house_types = extract_house_tokens(tokenize(string))
    house = ''
    for token, type in zip(house_tokens, house_types):
        if type == 'квартира':
            break
        else:
            house += f'{token} ' if type in ('число', 'литера') else ''
    split = tokens_to_string(house_tokens, string)
    address_string = string[:split].lower()
    matches = addr_extractor(address_string)
    facts = [i.fact.as_json for i in matches]
    tokens = []
    flat = None
    for fact in facts:
        name, type = fact['value'], fact['type'] if 'type' in fact else None
        if name not in tokens and type not in ('страна', 'квартира', 'дом'):
            tokens.append(name.lower())
        if 'type' in fact and fact['type'] == 'квартира':
            flat = name
    return {
        'postal_code': index,
        'flat': flat,
        'raw_address': f"{address_string.strip()} {house.strip()}",
        'search_string': f"{' '.join(tokens)} {house.strip()}"
    }
