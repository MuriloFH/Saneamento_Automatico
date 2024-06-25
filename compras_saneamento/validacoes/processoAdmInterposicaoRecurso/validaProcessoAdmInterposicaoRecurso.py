from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl

def processoAdmInterposicaoRecurso(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       situacao_interposicao_recurso_invalida=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_situacao_interposicao_recurso_invalida(pre_validacao):
        nomeValidacao = "A interposição de recurso do fornecedor do processo precisa estar acatada ou rejeitada"

        def analisa_situacao_interposicao_recurso_invalida():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_situacao_interposicao_recurso_invalida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    SELECT 
                         ir.i_entidades,
                         ir.i_ano_proc,
                         ir.i_processo,
                         ir.i_credores,
                         ir.i_tipo_julg,
                         ir.situacao_int,
                         homologado = (select isnull(p.data_homolog,'1900-01-01') 
                                        from compras.processos p 
                                        where p.i_processo = ir.i_processo and p.i_ano_proc = ir.i_ano_proc and p.i_entidades = ir.i_entidades)
                    FROM compras.interpor_recurso ir
                    JOIN compras.credores c ON (ir.i_entidades = c.i_entidades AND ir.i_credores = c.i_credores)
                    where ir.situacao_int not in (1,2)
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada situacao da interposicao de recurso processo {row['i_processo']}/{row['i_ano_proc']} credor {row['i_credores']} para 2-ACATADA")
                        comandoUpdate += f"""UPDATE compras.interpor_recurso set situacao_int = 2 where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_situacao_interposicao_recurso_invalida: {e}")
            return

        if situacao_interposicao_recurso_invalida:
            dado = analisa_situacao_interposicao_recurso_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_situacao_interposicao_recurso_invalida(listDados=dado)

    if dadosList:
        analisa_corrige_situacao_interposicao_recurso_invalida(pre_validacao="situacao_interposicao_recurso_invalida")
