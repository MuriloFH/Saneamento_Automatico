import datetime
import colorama
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def licencaPremio(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  dt_admissao_maior_dt_inicio_licenca=False,
                  dt_inicial_maior_dt_final_licenca=False,
                  qtd_dias_direito_nulo=False,
                  dt_inicial_e_final_licenca_nulo=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_admissao_maior_dt_inicio_licenca(pre_validacao):
        nomeValidacao = "Data da admissao maior que a data de inicio da licença"

        def analisa_dt_admissao_maior_dt_inicio_licenca():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_admissao_maior_dt_inicio_licenca(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDataInicial = ""
            dataFinal = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                            licencas_premio_per.i_entidades,
                                            licencas_premio_per.i_funcionarios,
                                            licencas_premio_per.i_licencas_premio,
                                            licencas_premio_per.i_licencas_premio_per,
                                            licencas_premio.num_dias_licenca,
                                            funcionarios.dt_admissao,
                                            licencas_premio_per.dt_inicial,
                                            licencas_premio_per.dt_final
                                            from bethadba.licencas_premio
                                            inner join bethadba.licencas_premio_per
                                               on licencas_premio.i_entidades = licencas_premio_per.i_entidades
                                               and licencas_premio.i_licencas_premio = licencas_premio_per.i_licencas_premio
                                               and licencas_premio.i_funcionarios  = licencas_premio_per.i_funcionarios
                                            inner join bethadba.funcionarios 
                                               on licencas_premio.i_entidades = funcionarios.i_entidades
                                               and licencas_premio.i_funcionarios  = funcionarios.i_funcionarios
                                            where funcionarios.dt_admissao > licencas_premio_per.dt_inicial
                                             order by licencas_premio_per.i_entidades, licencas_premio_per.i_funcionarios, licencas_premio_per.i_licencas_premio, licencas_premio_per.i_licencas_premio_per
                                        """)
                if len(busca) > 0:
                    for row in busca:
                        temPeriodoAnterior = banco.consultar(f"""SELECT *
                                                                    from bethadba.licencas_premio_per lpp 
                                                                    where lpp.i_funcionarios = {row['i_funcionarios']} 
                                                                    and lpp.i_licencas_premio = ({row['i_licencas_premio']}-1) 
                                                                    and lpp.i_licencas_premio_per = {row['i_licencas_premio_per']} 
                                                                    and lpp.i_entidades = {row['i_entidades']}
                                                            """)

                        if len(temPeriodoAnterior) > 0:
                            newDataInicial = temPeriodoAnterior[0]['dt_final']
                            newDataInicial += datetime.timedelta(days=1)
                        else:
                            newDataInicial = row['dt_admissao']

                        dadoAlterado.append(f"Alterado a data inicial da licença premio {row['i_licencas_premio']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"UPDATE bethadba.licencas_premio_per set dt_inicial = '{newDataInicial}' where i_funcionarios = {row['i_funcionarios']} and i_licencas_premio = {row['i_licencas_premio']} and i_licencas_premio_per = {row['i_licencas_premio_per']}  and i_entidades = {row['i_entidades']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_admissao_maior_dt_inicio_licenca: {e}")
            return

        if dt_admissao_maior_dt_inicio_licenca:
            dado = analisa_dt_admissao_maior_dt_inicio_licenca()

            if corrigirErros and len(dado) > 0:
                corrige_dt_admissao_maior_dt_inicio_licenca(listDados=dado)

    def analisa_corrige_dt_inicial_maior_dt_final_licenca(pre_validacao):
        nomeValidacao = "Data inicial da licença maior que a data final"

        def analisa_dt_inicial_maior_dt_final_licenca():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_inicial_maior_dt_final_licenca(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDataInicial = ""
            dataFinal = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_entidades,
                                             i_funcionarios,
                                             i_licencas_premio,
                                             i_licencas_premio_per,
                                             dt_inicial,
                                             dt_final 
                                            from bethadba.licencas_premio_per lpp 
                                                where dt_inicial > dt_final
                                            order by 1,2,3,4,5 asc
                                        """)

                if len(busca) > 0:
                    for row in busca:

                        temPeriodoAnterior = banco.consultar(f"""SELECT *
                                                                    from bethadba.licencas_premio_per lpp 
                                                                    where lpp.i_funcionarios = {row['i_funcionarios']} 
                                                                    and lpp.i_licencas_premio = ({row['i_licencas_premio']}-1) 
                                                                    and lpp.i_licencas_premio_per = {row['i_licencas_premio_per']} 
                                                                    and lpp.i_entidades = {row['i_entidades']}
                                                            """)

                        if len(temPeriodoAnterior) > 0:
                            newDataInicial = temPeriodoAnterior[0]['dt_final']
                            newDataInicial += datetime.timedelta(days=1)
                        else:
                            newDataInicial = row['dt_admissao']

                        dadoAlterado.append(f"Alterado a data inicial da licença premio {row['i_licencas_premio']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"UPDATE bethadba.licencas_premio_per set dt_inicial = '{newDataInicial}' where i_funcionarios = {row['i_funcionarios']} and i_licencas_premio = {row['i_licencas_premio']} and i_licencas_premio_per = {row['i_licencas_premio_per']}  and i_entidades = {row['i_entidades']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_inicial_maior_dt_final_licenca: {e}")
            return

        if dt_inicial_maior_dt_final_licenca:
            dado = analisa_dt_inicial_maior_dt_final_licenca()

            if corrigirErros and len(dado) > 0:
                corrige_dt_inicial_maior_dt_final_licenca(listDados=dado)

    def analisa_corrige_qtd_dias_direito_nulo(pre_validacao):
        nomeValidacao = "Quantidade de dias de direito não informado"

        def analisa_qtd_dias_direito_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_qtd_dias_direito_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select lp.i_entidades,
                                                     lp.i_funcionarios,
                                                     lp.i_licencas_premio,
                                                     lp.num_dias_licenca
                                            from bethadba.licencas_premio lp
                                             where (num_dias_licenca  is null or  num_dias_licenca < 0)
                                             order by 1,2,3 asc
                                        """)
                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Adicionado 90 dias de direito na licença {row['i_licencas_premio']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.licencas_premio set num_dias_licenca = 90 where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_licencas_premio = {row['i_licencas_premio']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_qtd_dias_direito_nulo: {e}")
            return

        if qtd_dias_direito_nulo:
            dado = analisa_qtd_dias_direito_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_qtd_dias_direito_nulo(listDados=dado)

    def analisa_corrige_dt_inicial_e_final_licenca_nulo(pre_validacao):
        nomeValidacao = "Data inicial e/ou final da licença não informada"

        def analisa_dt_inicial_e_final_licenca_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_inicial_e_final_licenca_nulo(listDados):
            tipoCorrecao = "INSERSAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select lpp.i_entidades,
                                             lpp.i_funcionarios,
                                             lpp.i_licencas_premio,
                                             lpp.i_licencas_premio_per,
                                             lpp.num_dias,
                                             f.dt_admissao
                                            from bethadba.licencas_premio_per lpp 
                                            join bethadba.funcionarios f on (f.i_entidades = lpp.i_entidades and f.i_funcionarios = lpp.i_funcionarios)
                                            where (dt_inicial is null or dt_final is null) 
                                            and i_averbacoes is null and status = 'S' 
                                            and lpp.observacao not like '%averb%'
                                            order by 1,2,3,4
                                        """)
                newDtInicial = ""
                newDtFinal = ""
                if len(busca) > 0:
                    for row in busca:

                        temPeriodoAnterior = banco.consultar(f"""SELECT *
                                                                    from bethadba.licencas_premio_per lpp 
                                                                    where lpp.i_funcionarios = {row['i_funcionarios']} 
                                                                    and lpp.i_licencas_premio = ({row['i_licencas_premio']}-1) 
                                                                    and lpp.i_licencas_premio_per = {row['i_licencas_premio_per']} 
                                                                    and lpp.i_entidades = {row['i_entidades']}
                                                            """)

                        if len(temPeriodoAnterior) > 0:
                            if temPeriodoAnterior[0]['dt_final'] is not None:
                                newDtInicial = temPeriodoAnterior[0]['dt_final']
                                newDtInicial += datetime.timedelta(days=1)

                                newDtFinal = newDtInicial
                                newDtFinal += datetime.timedelta(days=(row['num_dias'] - 1))
                            else:
                                print(colorama.Fore.RED, f"Licença {row['i_licencas_premio']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']} não ajustada. Favor analisar manualmente, pois a data_fim pode estar nula e não atender os requisitos da correção.", colorama.Fore.RESET)
                                continue
                        else:
                            newDtInicial = row['dt_admissao']
                            newDtFinal = row['dt_admissao']
                            newDtFinal += datetime.timedelta(days=(row['num_dias'] - 1))

                        dadoAlterado.append(f"Inserido a data inicial {newDtInicial} e final {newDtFinal} para a licença {row['i_licencas_premio']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                        comandoUpdate = f"""UPDATE bethadba.licencas_premio_per set dt_inicial = '{newDtInicial}', dt_final = '{newDtFinal}'
                                                where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_licencas_premio = {row['i_licencas_premio']};\n"""

                        banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_inicial_e_final_licenca_nulo: {e}")
            return

        if dt_inicial_e_final_licenca_nulo:
            dado = analisa_dt_inicial_e_final_licenca_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_dt_inicial_e_final_licenca_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_dt_admissao_maior_dt_inicio_licenca(pre_validacao="dt_admissao_maior_dt_inicio_licenca")
        analisa_corrige_dt_inicial_maior_dt_final_licenca(pre_validacao="dt_inicial_maior_dt_final_licenca")
        analisa_corrige_qtd_dias_direito_nulo(pre_validacao='qtd_dias_direito_nulo')
        analisa_corrige_dt_inicial_e_final_licenca_nulo(pre_validacao='dt_inicial_e_final_licenca_nulo')
