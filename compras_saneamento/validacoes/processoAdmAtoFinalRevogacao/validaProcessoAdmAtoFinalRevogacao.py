from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp
import polars as pl

def processoAdmAtoFinalRevogacao(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                            corrigirErros=False,
                            responsavel_revogacao_nao_informado=False
                           ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)


    def analisa_corrige_responsavel_revogacao_nao_informado(pre_validacao):
        nomeValidacao = "O responsável pela anulação/revogação do processo não foi informado!"

        def analisa_responsavel_revogacao_nao_informado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_responsavel_revogacao_nao_informado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    select distinct
                         a.i_entidades,
                         a.i_responsaveis_atos,
                         r.cpf
                    from compras.anl_processos a
                    join compras.responsaveis_atos r on r.i_responsaveis_atos = a.i_responsaveis_atos and r.i_entidades = a.i_entidades
                    where r.cpf = '' or r.cpf is null

                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    df = df.with_columns(
                        pl.col('i_responsaveis_atos').map_elements(lambda x:geraCfp()).alias('novo_cpf')
                    )

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado CPF do responsável {row['i_responsaveis_atos']} para {row['novo_cpf']}")
                        comandoUpdate += f"""UPDATE compras.responsaveis_atos set cpf = '{row['novo_cpf']}' where i_responsaveis_atos = {row['i_responsaveis_atos']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_responsavel_revogacao_nao_informado: {e}")
            return

        if responsavel_revogacao_nao_informado:
            dado = analisa_responsavel_revogacao_nao_informado()

            if corrigirErros and len(dado) > 0:
                corrige_responsavel_revogacao_nao_informado(listDados=dado)

    if dadosList:
        analisa_corrige_responsavel_revogacao_nao_informado(pre_validacao="responsavel_revogacao_nao_informado")
