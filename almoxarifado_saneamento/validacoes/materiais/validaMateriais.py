from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import pandas as pd


def materiais(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   materiais_descricao_duplicada=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_materiais_descricao_duplicada(pre_validacao):
        nomeValidacao = "Materiais com descrição duplicada"

        def analisa_materiais_descricao_duplicada():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_materiais_descricao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select 
                                                m.descricao , count(*) as quantidade, list(i_material) as materiais_duplicados
                                            from bethadba.materiais m 
                                            group by m.descricao
                                            HAVING count(*) > 1    
                                         """)

                if len(busca) > 0:
                    df = pd.DataFrame(busca)
                    materiais_unicos = [item for sublist in df['materiais_duplicados'] for item in sublist.split(',')]
                    for i in materiais_unicos:
                        dadoAlterado.append(f"Alterada descricao do material {i}")
                        comandoUpdate += f"""update bethadba.materiais set descricao = i_material || ' - ' || descricao where i_material = ({i});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_materiais_descricao_duplicada: {e}")
            return

        if materiais_descricao_duplicada:
            dado = analisa_materiais_descricao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_materiais_descricao_duplicada(listDados=dado)


    if dadosList:
        analisa_corrige_materiais_descricao_duplicada(pre_validacao="materiais_descricao_duplicada")





