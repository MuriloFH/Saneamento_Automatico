from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp
import polars as pl


def atos(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
         corrigirErros=False,
         ato_nulo=False,
         ato_data_designacao_ou_publicacao_nula=False
         ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_ato_nulo(pre_validacao):
        nomeValidacao = "O número do ato não foi informado no cadastro."

        def analisa_ato_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ato_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                                           with cte as(
                                                select 
                                                    i_atos_legais as id,
                                                    isnull(compras.dbf_retira_caracteres(nr_ato),'') as numero,
                                                    'atos legais' as origem,
                                                    'atos_legais' as nome_tabela,
                                                    'i_atos_legais' as id_registro,
                                                    'nr_ato' as nome_campo,
                                                    i_entidades 
                                                FROM compras.atos_legais
                                                union
                                                select 
                                                    i_responsavel as id, 
                                                    isnull(compras.dbf_retira_caracteres(portaria_comissao),'') as numero,
                                                    'dados dos responsáveis' as origem,
                                                    'responsaveis' as nome_tabela,
                                                    'i_responsavel' as id_registro,
                                                    'portaria_comissao' as nome_campo,
                                                    i_entidades
                                                from compras.responsaveis
                                            )select 
                                                cte.id,
                                                cte.origem,
                                                cte.nome_tabela,
                                                cte.id_registro,
                                                cte.nome_campo,
                                                cte.i_entidades
                                            from 
                                                cte 
                                            where numero = ''
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado o número do ato no cadastro de {row['origem']}, código {row['id']}")
                        comandoUpdate += f"""UPDATE compras.{row['nome_tabela']} set {row['nome_campo']} = '001' where {row['id_registro']} = {row['id']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ato_nulo: {e}")
            return

        if ato_nulo:
            dado = analisa_ato_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_ato_nulo(listDados=dado)

    def analisa_corrige_ato_data_designacao_ou_publicacao_nula(pre_validacao):
        nomeValidacao = "A data de designação ou de publicação não foi informada no cadastro."

        def analisa_ato_data_designacao_ou_publicacao_nula():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ato_data_designacao_ou_publicacao_nula(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""with cte as(
                                                select 
                                                    i_atos_legais as id,
                                                    isnull(dateformat(data_criacao,'yyyy-mm-dd'),'1900-01-01') as dataCriacao,
                                                    'atos legais' as origem,
                                                    'atos_legais' as nome_tabela,
                                                    'i_atos_legais' as id_registro,
                                                    'data_criacao' as nome_campo,
                                                    i_entidades 
                                                FROM compras.atos_legais
                                                union
                                                select 
                                                    i_responsavel as id, 
                                                    isnull(dateformat(data_publ,'yyyy-mm-dd'),isnull(dateformat(data_desig,'yyyy-mm-dd'),'1900-01-01')) as dataCriacao,
                                                    'dados dos responsáveis' as origem,
                                                    'responsaveis' as nome_tabela,
                                                    'i_responsavel' as id_registro,
                                                    'data_publ' as nome_campo,
                                                    i_entidades
                                                from compras.responsaveis
                                            )select 
                                                cte.id,
                                                case 
                                                    when cte.nome_tabela = 'responsaveis'
                                                    then coalesce(
                                                                (select dateformat(min(p.data_processo),'yyyy-mm-dd')
                                                                from compras.processos p
                                                                where p.i_responsavel = cte.id 
                                                                and p.i_entidades = cte.i_entidades),
                                                                dateformat(now() - 1825,'yyyy-mm-dd')
                                                            )
                                                end as nova_dataCriacao,	
                                                cte.origem,
                                                cte.nome_tabela,
                                                cte.id_registro,
                                                cte.nome_campo,
                                                cte.i_entidades
                                            from 
                                                cte 
                                            where cte.dataCriacao = '1900-01-01'
                    """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de designação ou de publicação no cadastro de {row['origem']}, código {row['id']}")
                        comandoUpdate += f"""UPDATE compras.{row['nome_tabela']} set {row['nome_campo']} = '{row['nova_dataCriacao']}' where {row['id_registro']} = {row['id']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ato_data_designacao_ou_publicacao_nula: {e}")
            return

        if ato_data_designacao_ou_publicacao_nula:
            dado = analisa_ato_data_designacao_ou_publicacao_nula()

            if corrigirErros and len(dado) > 0:
                corrige_ato_data_designacao_ou_publicacao_nula(listDados=dado)

    if dadosList:
        analisa_corrige_ato_nulo(pre_validacao="ato_nulo")
        analisa_corrige_ato_data_designacao_ou_publicacao_nula(pre_validacao="ato_data_designacao_ou_publicacao_nula")
