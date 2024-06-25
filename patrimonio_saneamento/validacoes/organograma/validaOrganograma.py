from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def organograma(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                descricao_unidade_maior_60_caracter=False):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_descricao_unidade_maior_60_caracter(pre_validacao):
        nomeValidacao = "Descrição unidade maior que 60 caracteres"

        def analisa_descricao_unidade_maior_60_caracter():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_descricao_unidade_maior_60_caracter(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT i_unidade as unidade, i_unid_orc as unidadeOrcamentaria, descricao
                                            from bethadba.unidades_orc uo 
                                            where LENGTH(uo.descricao) > 60
                                        """)

                if len(busca) > 0:
                    for i in busca:
                        newDescricao = i['descricao'][:60]

                        dadoAlterado.append(f"Feito a delimitação da descrição da unidade orçamentária {i['unidadeOrcamentaria']} da unidade {i['unidade']}")

                        comandoUpdate += f"UPDATE bethadba.unidades_orc SET descricao='{newDescricao}' WHERE i_unidade= {i['unidade']} and i_unid_orc = {i['unidadeOrcamentaria']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_unidade_maior_60_caracter: {e}")

        if descricao_unidade_maior_60_caracter:
            dado = analisa_descricao_unidade_maior_60_caracter()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_unidade_maior_60_caracter(listDados=dado)

    if dadosList:
        analisa_corrige_descricao_unidade_maior_60_caracter(pre_validacao="descricao_unidade_maior_60_caracter")
