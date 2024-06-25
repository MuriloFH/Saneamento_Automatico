from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import pandas as pd


def lotesMateriais(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   data_fabricacao_maior_que_data_de_validade=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_fabricacao_maior_que_data_de_validade(pre_validacao):
        nomeValidacao = "Lote possui a data de fabricação maior que a data de validade."

        def analisa_data_fabricacao_maior_que_data_de_validade():

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

        def corrige_data_fabricacao_maior_que_data_de_validade(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select 
                                                l.i_lote,
                                                l.datafabricacao,
                                                l.datavalidade,
                                                year(l.datafabricacao) + 5 || RIGHT(l.datafabricacao,6) as nova_validade,	-- Data de validade adicionado 5 anos
                                                l.i_entidades
                                            from bethadba.lotes l
                                            where l.datafabricacao > l.datavalidade
                                         """)

                if len(busca) > 0:
                    df = pd.DataFrame(busca)
                    for row in df.itertuples():
                        dadoAlterado.append(f"Alterada data de validade do lote {row.i_lote} de {row.datavalidade} para {row.nova_validade}")
                        comandoUpdate += f"""update bethadba.lotes set datavalidade = '{row.nova_validade}' where i_lote = {row.i_lote} and i_entidades = {row.i_entidades};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_fabricacao_maior_que_data_de_validade: {e}")
            return

        if data_fabricacao_maior_que_data_de_validade:
            dado = analisa_data_fabricacao_maior_que_data_de_validade()

            if corrigirErros and len(dado) > 0:
                corrige_data_fabricacao_maior_que_data_de_validade(listDados=dado)


    if dadosList:
        analisa_corrige_data_fabricacao_maior_que_data_de_validade(pre_validacao="data_fabricacao_maior_que_data_de_validade")





