import re


stopwords = {
    'российская': '',
    'федерация': '',
    'россия': '',
    'мо': 'московская обл',
    'большой': "б",
    'большая': "б",
    'малый': "м",
    'малая': "м",
    'средний': 'ср',
    'средняя': 'cр',
    'нижний': 'н',
}

replaces = {
    'обл': ["область", "обл", "обл-ть"],
    'респ': ["республика", 'респ'],
    'край': ['край'],
    'г': ['г', 'гор', 'город'],
    'ао': ['автономный округ', "автономный", 'аокр', 'а.окр'],
    'аобл': ['автономная область', 'авт.обл', 'аобл', 'а обл', 'аобл'],
    'ал': ['аллея', 'а', 'ал'],
    'б-р': ['б-р', 'бульвар'],
    'наб': ['наб', 'набережная'],
    'пер': ['пер', 'переулок'],
    'пл': ['пл', "площадь"],
    'пр-кт': ["проспект", "пр", "пр-кт", "просп", 'пр-т'],
    "проезд": ["проезд", "пр-д", "прд"],
    "ул": ["улица", "ул", "у", 'ул-ца'],
    'р-н': ['район', "р", "р-н"],
    'п': ['поселок', 'посёлок', "пос"],
    'пгт': ['поселок городского типа', 'посёлок городского типа', 'пос. гор. типа', 'пос.гор.типа', 'пос гор типа'],
    'с': ['с', 'село', 'сел'],
    'д': ['д', 'дер', 'деревня', 'д-ня'],
    'с/п': ["сельский поселок", "сельский посёлок", "сп", 'сельское поселение', 'сельпо', 'сп', 'сел.п.', ],
    'д': ['д', 'дом'],
    'влд': ['владение', 'вл', 'влд'],
    'двлд': ['домовладение', 'двл', 'двлд'],
    'к': ['к', 'корп', 'копр', 'кор', 'корпус'],
    'стр': ['с', 'стр', 'строен', 'строение'],
    'кв': ['кв', 'квартира'],
    'пом': ['пом', 'помещение'],
    'ком': ["ком", 'комн', 'комната'],
    'каб': ["кабинет", "каб", "к-т", "каб-т"],
    'оф': ['оф', 'офис'],
    '': ['литер', 'литера', 'лит'],
}

replaces_dict = dict()

for replace_with, chars_to_replace in replaces.items():
    for char_to_replace in chars_to_replace:
        replaces_dict[char_to_replace] = replace_with


def replace_special_chars_with_space(string):
    return re.sub(r'\W', ' ', string)


def preprocess(string):
    '''
    Отделяет всё что можно друг от друга чтобы облегчить токенизацию
    '''
    string = replace_special_chars_with_space(string)
    # Превращает "2c3" в "2 c 3"  и  "2-3" в "2 - 3"
    string = re.sub(r"[\d]+|[\W]+", ' \g<0> ', string)
    # то же самое, только с запятыми
    string = string.replace(',', ', ')
    string = re.sub(r'\,|\.|\-|\'|\"|\(|\)', '', string)
    string = re.sub(r' +', ' ', string)
    return string


# def multiple_replace(text: str, replace_dictionary: dict):
#     for key, value in replace_dictionary.items():
#         text = text.replace(key, value)
#     return text


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


def find_tokens_in_string(tokens, string):
    '''
    Ищет токены в строке и возвращает их позицию начала
    '''
    pattern = r".?.?".join(tokens)
    found = re.search(pattern, string.lower())
    if found is None:
        split = len(string)
    else:
        split = found.start()
    return split


def extract_index(string, errors=False):  # 100% works !!!
    '''
    Извлекает индекс из строки
    Вход: строка
    Выход: адрес без индекса, индекс
    '''
    index = re.findall(r'[^| |,][\d]{5}[ |$|, ]', string)
    if len(index) > 1 and errors:
        print("Два индекса в строке \"%s\" ?" % string)

    if index != []:
        index = index[0]
        string = string.replace(index, '').strip()
        index = index.replace(',', '')
    else:
        index = None
    return string, index


def extract_flat(string):
    if 'кв' in string:
        flat = re.findall(r'\d+', string)[-1].strip()
        string = string.replace('кв ', '')
        string = string.replace(flat, '')
    else:
        flat = None
    return string, flat


def optimize_for_search(string):
    '''
    вводит небольшие изменения в строку поиска для более точного поиска
    '''
    string = string.replace('ё', 'е').lower()
    tokens = [t for t in string.split(' ') if t]
    new_tokens = []
    for token in tokens:
        if token in stopwords:
            token = stopwords[token]
        if token in replaces_dict:
            token = replaces_dict[token]
        new_tokens.append(token)
    return ' '.join(new_tokens)
