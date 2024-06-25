from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp
import polars as pl


def responsaveis(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 responsavel_cpf_nulo_ou_invalido=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_responsavel_cpf_nulo_ou_invalido(pre_validacao):
        nomeValidacao = "O responsável não possui CPF informado ou é invalido."

        def analisa_responsavel_cpf_nulo_ou_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_responsavel_cpf_nulo_ou_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""with cte_respons as (
                                                -- Responsável ato
                                                select i_responsaveis_atos as i_responsavel, cpf, 'responsaveis_atos' as tabela_origem, 'i_responsaveis_atos' as campo_id, 'cpf' as campo_cpf, i_entidades
                                                from compras.responsaveis_atos
                                                union 
                                                -- Responsável dos credores
                                                select i_credores as i_responsavel, cpf_responsavel as cpf, 'credores' as tabela_origem, 'i_credores' as campo_id, 'cpf_responsavel' as campo_cpf, i_entidades
                                                from compras.credores
                                                union
                                                -- Titular - Cpf que não estejam em compras.responsaveis_atos
                                                select  i_responsavel, cpf_titular as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_titular' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_titular not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Presidente
                                                select i_responsavel, cpf_presid as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_presid' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_presid not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 01
                                                select i_responsavel, cpf_membro1 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro1' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro1 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 02
                                                select i_responsavel, cpf_membro2 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro2' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro2 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 03
                                                select i_responsavel, cpf_membro3 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro3' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro3 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 04
                                                select i_responsavel, cpf_membro4 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro4' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro4 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 05
                                                select i_responsavel, cpf_membro5 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro5' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro5 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 06
                                                select i_responsavel, cpf_membro6 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro6' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro6 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 07
                                                select i_responsavel, cpf_membro7 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro7' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro7 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                                union
                                                -- Membro 08
                                                select i_responsavel, cpf_membro8 as cpf, 'responsaveis' as tabela_origem, 'i_responsavel' as campo_id, 'cpf_membro8' as campo_cpf, i_entidades
                                                from compras.responsaveis
                                                where cpf_membro8 not in(select r.cpf from compras.responsaveis_atos r where r.cpf = cpf and r.i_entidades = responsaveis.i_entidades)
                                            )select 
                                                i_responsavel,
                                                cpf,
                                                tabela_origem,
                                                campo_id,
                                                campo_cpf,
                                                i_entidades,
                                                compras.dbf_valida_cgc_cpf(null,cpf,'F','S') as valida_cpf
                                            from 
                                                cte_respons
                                            where valida_cpf = 0    
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df = df.with_columns(
                        pl.col('i_responsavel').map_elements(lambda x: geraCfp()).alias('novo_cpf')
                    )

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado CPF  do responsável (tabela {row['tabela_origem']}/{row['campo_id']}) {row['i_responsavel']} para {row['novo_cpf']}")
                        comandoUpdate += f"""UPDATE compras.{row['tabela_origem']} set {row['campo_cpf']} = '{row['novo_cpf']}' where {row['campo_id']} = {row['i_responsavel']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_responsavel_cpf_nulo_ou_invalido: {e}")
            return

        if responsavel_cpf_nulo_ou_invalido:
            dado = analisa_responsavel_cpf_nulo_ou_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_responsavel_cpf_nulo_ou_invalido(listDados=dado)

    if dadosList:
        analisa_corrige_responsavel_cpf_nulo_ou_invalido(pre_validacao="responsavel_cpf_nulo_ou_invalido")
