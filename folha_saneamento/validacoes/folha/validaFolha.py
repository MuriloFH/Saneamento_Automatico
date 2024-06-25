from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def folha(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
          corrigirErros=False,
          folha_sem_fechamento=False
          ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_folha_sem_fechamento(pre_validacao):
        nomeValidacao = "Folha sem data de fechamento."

        def analisa_folha_sem_fechamento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_folha_sem_fechamento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_entidades, 
                        i_competencias,
                        i_tipos_proc,
                        i_processamentos,
                        dt_fechamento,
                        new_dt_fechamento = date(year(i_competencias)||
                        (if month(i_competencias) < 10 then '0'+cast(month(i_competencias) as varchar) else cast(month(i_competencias) as varchar) endif)||
                        (if month(i_competencias) = 2 then '28' else '30' endif)),
                        'processamentos' as tabela
                    FROM bethadba.processamentos 
                    WHERE dt_fechamento is null  
                    
                    union
                    
                    SELECT 
                        i_entidades, 
                        i_competencias,
                        i_tipos_proc,
                        i_processamentos,
                        dt_fechamento,
                        new_dt_fechamento = date(year(i_competencias)||
                        (if month(i_competencias) < 10 then '0'+cast(month(i_competencias) as varchar) else cast(month(i_competencias) as varchar) endif)||
                        (if month(i_competencias) = 2 then '28' else '30' endif)),
                        'dados_calc' as tabela
                    FROM bethadba.dados_calc 
                    WHERE dt_fechamento is null   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        # Esta tratativa leva em consideração a dt_fechamento das tabelas processamentos e dados_calc
                        dadoAlterado.append(
                            f"Alterada data de fechamento da folha: Competencia {row['i_competencias']}, tipo processamento {row['i_tipos_proc']}, processamento {row['i_processamentos']}, tabela {row['tabela']} entidade {row['i_entidades']} para {row['new_dt_fechamento']}")
                        comandoUpdate += f"""UPDATE bethadba.{row['tabela']} set dt_fechamento = '{row['new_dt_fechamento']}' where i_entidades = {row['i_entidades']} and i_competencias = '{row['i_competencias']}' and i_tipos_proc = {row['i_tipos_proc']} and i_processamentos = {row['i_processamentos']} and dt_fechamento is null;\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_folha_sem_fechamento: {e}")
            return

        if folha_sem_fechamento:
            dado = analisa_folha_sem_fechamento()

            if corrigirErros and len(dado) > 0:
                corrige_folha_sem_fechamento(listDados=dado)

    if dadosList:
        analisa_corrige_folha_sem_fechamento(pre_validacao="folha_sem_fechamento")
