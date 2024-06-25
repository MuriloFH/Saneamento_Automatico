import os


def comandoPonto():
    partial_path = os.path.split(os.path.realpath(__file__))[0]
    path = os.path.join(partial_path, 'populando_tabela_controle.sql')

    with open(path, 'r', encoding='utf8') as file:
        query = file.read()
        return query
