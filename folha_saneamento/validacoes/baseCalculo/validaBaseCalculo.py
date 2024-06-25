from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def baseCalculo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 bases_calc_outras_empresas_vigencia_invalida=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_bases_calc_outras_empresas_vigencia_invalida(pre_validacao):
        nomeValidacao = "Vigencia maior que 2099 em bases de calculos de outras empresas"

        def analisa_bases_calc_outras_empresas_vigencia_invalida():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_bases_calc_outras_empresas_vigencia_invalida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_pessoas,
                        dt_vigencia_ini,
                        dt_vigencia_fin,
                        i_sequencial 
                    FROM 
                        bethadba.bases_calc_outras_empresas
                    WHERE 
                        dt_vigencia_fin > '2099-01-01'   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada vigência da base de calculo de outras empresas: pessoa {row['i_pessoas']}, vigencia inicial {row['dt_vigencia_fin']} para 2099-01-01")
                        comandoUpdate += f"""UPDATE bethadba.bases_calc_outras_empresas set dt_vigencia_fin = '2099-01-01' where i_pessoas = {row['i_pessoas']} and dt_vigencia_ini = '{row['dt_vigencia_ini']}' and i_sequencial = {row['i_sequencial']} and dt_vigencia_fin > '2099-01-01';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_bases_calc_outras_empresas_vigencia_invalida: {e}")
            return

        if bases_calc_outras_empresas_vigencia_invalida:
            dado = analisa_bases_calc_outras_empresas_vigencia_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_bases_calc_outras_empresas_vigencia_invalida(listDados=dado)

    if dadosList:
        analisa_corrige_bases_calc_outras_empresas_vigencia_invalida(pre_validacao="bases_calc_outras_empresas_vigencia_invalida")
