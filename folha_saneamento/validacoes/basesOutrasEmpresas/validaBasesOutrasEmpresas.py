from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def basesOutrasEmpresas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        dt_fim_divergente=False
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_fim_divergente(pre_validacao):
        nomeValidacao = "Data fim longa."

        def analisa_dt_fim_divergente():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_fim_divergente(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select  i_entidades ,i_pessoas, i_sequencial, dt_vigencia_ini , dt_vigencia_fin  
                                            from bethadba.bases_calc_outras_empresas 
                                            where dt_vigencia_fin >= date(dateadd(year,100,GETDATE()))
                                            order by i_pessoas, i_sequencial
                                        """)
                if len(busca) > 0:
                    for row in busca:
                        newDtFinal = row['dt_vigencia_ini']

                        dadoAlterado.append(f"Alterado a data fim {newDtFinal} para a pessoa {row['i_pessoas']} no sequencial {row['i_sequencial']} com a data inicial da vigência {row['dt_vigencia_ini']}")
                        comandoUpdate += f"""UPDATE bethadba.bases_calc_outras_empresas set dt_vigencia_fin = '{newDtFinal}' where i_pessoas = {row['i_pessoas']} and i_sequencial = {row['i_sequencial']} and dt_vigencia_ini = '{row['dt_vigencia_ini']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_fim_divergente: {e}")
            return

        if dt_fim_divergente:
            dado = analisa_dt_fim_divergente()

            if corrigirErros and len(dado) > 0:
                corrige_dt_fim_divergente(listDados=dado)

    if dadosList:
        analisa_corrige_dt_fim_divergente(pre_validacao="dt_fim_divergente")
