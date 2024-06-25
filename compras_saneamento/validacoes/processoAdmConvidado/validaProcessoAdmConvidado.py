from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl

def processoAdmConvidado(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       data_protocolo_convidado_nula=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_protocolo_convidado_nula(pre_validacao):
        nomeValidacao = "A data do protocolo do fornecedor convidado não foi informada (referente a data do convite no cloud)"

        def analisa_data_protocolo_convidado_nula():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_protocolo_convidado_nula(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    select 
                        c.i_entidades,
                        c.i_ano_proc,
                        c.i_processo,
                        c.i_credores,
                        c.data_protocolo,
                        c.data_recibo,
                        p.data_processo
                    from compras.convidados c
                    join compras.processos p on p.i_entidades = c.i_entidades and p.i_ano_proc = c.i_ano_proc and p.i_processo = c.i_processo
                    where data_protocolo is null
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data do protocolo do convidado {row['i_credores']} do processo {row['i_processo']}/{row['i_ano_proc']} para {row['data_processo']}")
                        comandoUpdate += f"""UPDATE compras.convidados set data_protocolo = '{row['data_processo']}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_protocolo_convidado_nula: {e}")
            return

        if data_protocolo_convidado_nula:
            dado = analisa_data_protocolo_convidado_nula()

            if corrigirErros and len(dado) > 0:
                corrige_data_protocolo_convidado_nula(listDados=dado)

    if dadosList:
        analisa_corrige_data_protocolo_convidado_nula(pre_validacao="data_protocolo_convidado_nula")
