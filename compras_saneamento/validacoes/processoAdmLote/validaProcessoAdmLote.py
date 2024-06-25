from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import pandas as pd

def processoAdmLote(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       lote_processo_adm_duplicidade_descricao=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_lote_processo_adm_duplicidade_descricao(pre_validacao):
        nomeValidacao = "O lote de processo adminsitrativo, possui lotes diferentes com mesma descrição"

        def analisa_lote_processo_adm_duplicidade_descricao():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_lote_processo_adm_duplicidade_descricao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    select 
                        trim(lo.obs_lote) as descricao,
                        count(*) as quantidade,
                        lo.i_ano_proc,
                        lo.i_processo,
                        list(lo.i_lotes) as lotes_duplicados,
                        lo.i_entidades
                    from compras.lotes lo 
                    group by lo.i_entidades, lo.i_ano_proc, lo.i_processo, descricao 
                    HAVING count(*) > 1
                """)

                if len(listDados) > 0:
                    df = pd.DataFrame(busca)
                    for index, row in df.iterrows():
                        lotes = [item for sublist in row['lotes_duplicados'] for item in sublist.strip(',')]
                        for lote in lotes:
                            nova_descricao = row['descricao'] + ' ' + str(lote)
                            dadoAlterado.append(f"Alterada descricao do lote {lote} o processo {row.i_processo}/{row.i_ano_proc}")
                            comandoUpdate += f"""UPDATE compras.lotes set obs_lote = '{nova_descricao}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_lotes = {lote} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_lote_processo_adm_duplicidade_descricao: {e}")
            return

        if lote_processo_adm_duplicidade_descricao:
            dado = analisa_lote_processo_adm_duplicidade_descricao()

            if corrigirErros and len(dado) > 0:
                corrige_lote_processo_adm_duplicidade_descricao(listDados=dado)

    if dadosList:
        analisa_corrige_lote_processo_adm_duplicidade_descricao(pre_validacao="lote_processo_adm_duplicidade_descricao")
