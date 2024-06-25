from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def locaisEntrega(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  local_entrega_nulo=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_local_entrega_nulo(pre_validacao):
        nomeValidacao = "Possui local de entrega nulo. Deverá ser informado um local de entrega"

        def analisa_local_entrega_nulo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            if dadosList:
                for i in dadosList:
                    if i['pre_validacao'] == f'{pre_validacao}':
                        dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_local_entrega_nulo(log=False):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            if log:
                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select i_ano_proc , i_processo , i_local, i_entidades 
                                            from compras.processos 
                                            where i_local is null
                                         """)
                local_minimo = banco.consultar("select min(i_local) as i_local FROM compras.local_entrega")

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    i_local = local_minimo[0]['i_local']
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Incluido local de entrega {i_local} para o processo {row['i_processo']}/{row['i_ano_proc']} entidade {row['i_entidades']}")
                        comandoUpdate += f"""update compras.processos set i_local = {i_local} where i_processo = {row['i_processo']} and i_ano_proc = {row['i_ano_proc']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                if log:
                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_local_entrega_nulo: {e}")
            return

        if local_entrega_nulo:
            dado = analisa_local_entrega_nulo()
            if local_entrega_nulo and len(dado) > 0:
                # a correção será chamada independente se foi localizado erros na tabela de controle
                corrige_local_entrega_nulo(log=True)
            elif local_entrega_nulo:
                corrige_local_entrega_nulo()

    analisa_corrige_local_entrega_nulo(pre_validacao="local_entrega_nulo")
