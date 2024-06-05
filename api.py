from parsing import optimize_for_search, optimize_housenum, preprocess, extract_index, extract_house

def standardize(string, origin=True, debug=False):
    '''
    Обёртка для всех методов выше. Разделяет адрес на его составляющие и ищет совпадение в ФИАС. В 90+% случаев находит.
    Вход: строка с адресом
    Выход: составляющие адреса
    '''
    dic = {}
    if origin:
        dic['origin'] = string

    string = preprocess(string)
    address, index = extract_index(string)
    try:
        index = index.strip()
    except AttributeError:
        pass
    address, house = extract_house(address)
    dic['index'] = index
    dic['address'] = address
    #dic.update(verify_address(address))
    #if dic.get('street', False):
    #    dic.update(verify_home(house, dic['guid'], index))
    dic.update(house)
    return dic


if __name__ == "__main__":
    address = standardize("Московская обл, Воскресенск г, Рабочая ул, дом 106, квартира 20")
    print(optimize_for_search(address['address']))
