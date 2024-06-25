from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def historicoSalarial(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                      corrigirErros=False,
                      historico_salarial_com_salario_zerado=False,
                      historico_com_horas_mes_zerada=False
                      ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_historico_salarial_com_salario_zerado(pre_validacao):
        nomeValidacao = "Históricos salariais com salário zerado ou nulo"

        def analisa_historico_salarial_com_salario_zerado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_historico_salarial_com_salario_zerado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_entidades, 
                        i_funcionarios, 
                        dt_alteracoes,
                        salario 
                    FROM bethadba.hist_salariais
                    WHERE salario IN (0, NULL)   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado Histórico Salarial {row['dt_alteracoes']} do funcionário {row['i_funcionarios']} entidade {row['i_entidades']} de {row['salario']} para 0.01")
                        comandoUpdate += f"""UPDATE bethadba.hist_salariais set salario = 0.01 where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_historico_salarial_com_salario_zerado: {e}")
            return

        if historico_salarial_com_salario_zerado:
            dado = analisa_historico_salarial_com_salario_zerado()

            if corrigirErros and len(dado) > 0:
                corrige_historico_salarial_com_salario_zerado(listDados=dado)

    def analisa_corrige_historico_com_horas_mes_zerada(pre_validacao):
        nomeValidacao = "Histórico salarial com horas mes zerada."

        def analisa_historico_com_horas_mes_zerada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_historico_com_horas_mes_zerada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        hs.i_entidades, 
                        hs.i_funcionarios, 
                        hs.dt_alteracoes
                    from bethadba.hist_salariais hs
                    join bethadba.funcionarios f
                    where cast(hs.horas_mes as integer) < 1
                    and tipo_func != 'A'  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado horas mes do historico salarial do funcionario {row['i_funcionarios']} data de alteracao {row['dt_alteracoes']} para 1")
                        comandoUpdate += f"""UPDATE bethadba.hist_salariais set horas_mes = 1 where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_historico_com_horas_mes_zerada: {e}")
            return

        if historico_com_horas_mes_zerada:
            dado = analisa_historico_com_horas_mes_zerada()

            if corrigirErros and len(dado) > 0:
                corrige_historico_com_horas_mes_zerada(listDados=dado)

    if dadosList:
        analisa_corrige_historico_salarial_com_salario_zerado(pre_validacao="historico_salarial_com_salario_zerado")
        analisa_corrige_historico_com_horas_mes_zerada(pre_validacao="historico_com_horas_mes_zerada")
