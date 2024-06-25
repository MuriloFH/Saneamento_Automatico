from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def localTrabalho(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  data_fim_menor_data_inicio=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_fim_menor_data_inicio(pre_validacao):
        nomeValidacao = "Lotação física com data fim menor do que a data inicio."

        def analisa_data_fim_menor_data_inicio():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_fim_menor_data_inicio(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_entidades,
                        i_funcionarios,
                        i_locais_trab,
                        dt_inicial,
                        dt_final
                    from bethadba.locais_mov
                    where dt_inicial > dt_final    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data inicial e final da Movimentação (Locais de Trabalho) do funcionario {row['i_funcionarios']} local de trabalho {row['i_locais_trab']} entidade {row['i_entidades']} para data inicial {row['dt_final']} e data final {row['dt_inicial']}")
                        comandoUpdate += f"""UPDATE bethadba.locais_mov set dt_inicial = '{row['dt_final']}', dt_final = '{row['dt_inicial']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_locais_trab = {row['i_locais_trab']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_fim_menor_data_inicio: {e}")
            return

        if data_fim_menor_data_inicio:
            dado = analisa_data_fim_menor_data_inicio()

            if corrigirErros and len(dado) > 0:
                corrige_data_fim_menor_data_inicio(listDados=dado)

    if dadosList:
        analisa_corrige_data_fim_menor_data_inicio(pre_validacao="data_fim_menor_data_inicio")
