import datetime
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def funcaoGratificada(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                      corrigirErros=False,
                      dt_inicial_dt_final_menor_dt_admissao=False
                      ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_inicial_dt_final_menor_dt_admissao(pre_validacao):
        nomeValidacao = "Data inicial e/ou final da função gratificada menor que adata de admissão"

        def analisa_dt_inicial_dt_final_menor_dt_admissao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_inicial_dt_final_menor_dt_admissao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDataInicial = ""
            dataFinal = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                            funcoes_func.i_entidades,
                                            funcoes_func.i_funcionarios,
                                            funcoes_func.i_funcoes_func,
                                            funcoes_func.i_funcoes_exerc,
                                            funcoes_func.dt_inicial,
                                            funcoes_func.dt_final,
                                            funcionarios.dt_admissao
                                            from
                                                bethadba.funcoes_func
                                                left join bethadba.funcionarios 
                                                on funcoes_func.i_funcionarios = funcionarios.i_funcionarios
                                                and funcoes_func.i_entidades = funcionarios.i_entidades
                                                where dt_inicial < dt_admissao or dt_final < dt_admissao
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        if row['dt_final'] is not None and row['dt_final'] < row['dt_admissao'] and row['dt_inicial'] < row['dt_admissao']:
                            newDtInicial = row['dt_admissao']
                            newDtFinal = row['dt_admissao']
                            newDtFinal += datetime.timedelta(days=30)

                            dadoAlterado.append(f"Alterado a data inicial para {newDtInicial} e a final para {newDtFinal} da função {row['i_funcoes_func']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE Folharh.bethadba.funcoes_func ff
                                                    SET dt_final = '{newDtFinal}', dt_inicial = '{newDtInicial}'
                                                    WHERE ff.i_funcionarios = {row['i_funcionarios']} and ff.i_funcoes_func = {row['i_funcoes_func']} and ff.i_entidades = {row['i_entidades']};\n"""

                        elif row['dt_inicial'] < row['dt_admissao']:
                            newDtInicial = row['dt_admissao']

                            dadoAlterado.append(f"Alterado a data inicial para {newDtInicial} da função {row['i_funcoes_func']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE Folharh.bethadba.funcoes_func ff
                                                    SET dt_inicial = '{newDtInicial}'
                                                    WHERE ff.i_funcionarios = {row['i_funcionarios']} and ff.i_funcoes_func = {row['i_funcoes_func']} and ff.i_entidades = {row['i_entidades']};\n"""

                        elif row['dt_final'] is not None and row['dt_final'] < row['dt_admissao']:
                            newDtFinal = row['dt_admissao']
                            newDtFinal += datetime.timedelta(days=30)

                            dadoAlterado.append(f"Alterado a data final para {newDtFinal} da função {row['i_funcoes_func']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE Folharh.bethadba.funcoes_func ff
                                                    SET dt_final = '{newDtFinal}'
                                                    WHERE ff.i_funcionarios = {row['i_funcionarios']} and ff.i_funcoes_func = {row['i_funcoes_func']} and ff.i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_inicial_dt_final_menor_dt_admissao: {e}")
            return

        if dt_inicial_dt_final_menor_dt_admissao:
            dado = analisa_dt_inicial_dt_final_menor_dt_admissao()

            if corrigirErros and len(dado) > 0:
                corrige_dt_inicial_dt_final_menor_dt_admissao(listDados=dado)

    if dadosList:
        analisa_corrige_dt_inicial_dt_final_menor_dt_admissao(pre_validacao="dt_inicial_dt_final_menor_dt_admissao")
