from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import pandas as pd


def unidadesMedida(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   unidade_medida_sem_abreviatura_ou_nome=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_unidade_medida_sem_abreviatura_ou_nome(pre_validacao):
        nomeValidacao = "Unidade de Medida não possui abreviatura ou nome informado"

        def analisa_unidade_medida_sem_abreviatura_ou_nome():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)


            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_unidade_medida_sem_abreviatura_ou_nome(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                               i_unidade,
                                               i_entidades,
                                               abreviatura,
                                               nome,
                                               CASE abreviatura
                                                when '' then 'AB' || i_unidade
                                                else abreviatura
                                               END as nova_abreviatura,
                                               CASE nome
                                                when '' then 'UNIDADE DE MEDIDA ' || i_unidade
                                                else nome
                                               END as novo_nome
                                            from bethadba.unidades
                                            where length(trim(abreviatura)) = 0 or length(trim(nome)) = 0
                                            ORDER BY unidades.i_unidade    
                                         """)

                if len(busca) > 0:
                    df = pd.DataFrame(busca)
                    # print(df)
                    for row in df.itertuples():
                        # print(row)
                        dadoAlterado.append(f"Alterado a unidade de medida {row.i_unidade}")
                        comandoUpdate += f"""UPDATE bethadba.unidades set abreviatura = '{row.nova_abreviatura}', nome = '{row.novo_nome}' where i_unidade = {row.i_unidade};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_unidade_medida_sem_abreviatura_ou_nome: {e}")
            return

        if unidade_medida_sem_abreviatura_ou_nome:
            dado = analisa_unidade_medida_sem_abreviatura_ou_nome()

            if corrigirErros and len(dado) > 0:
                corrige_unidade_medida_sem_abreviatura_ou_nome(listDados=dado)


    if dadosList:
        analisa_corrige_unidade_medida_sem_abreviatura_ou_nome(pre_validacao="unidade_medida_sem_abreviatura_ou_nome")





