import datetime
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def permutas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
             corrigirErros=False,
             dt_inicio_e_fim_nula=False
             ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_inicio_e_fim_nula(pre_validacao):
        nomeValidacao = "Data inicial e/ou final nula"

        def analisa_dt_inicio_e_fim_nula():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_inicio_e_fim_nula(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select pft.i_entidades, 
                                            pft.i_funcionarios, 
                                            f.dt_admissao,
                                            pft.i_turmas,
                                            pft.dt_inicial,
                                            pft.dt_final,
                                            t.dt_inicio_turma
                                            from bethadba.permuta_func_turmas pft 
                                            join bethadba.turmas t on (t.i_entidades = pft.i_entidades and t.i_turmas = pft.i_turmas)
                                            join bethadba.funcionarios f on (f.i_entidades = pft.i_entidades and f.i_funcionarios = pft.i_funcionarios)
                                            where dt_inicial is null or dt_final is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        if row['dt_inicial'] is None and row['dt_final'] is None:
                            newDtInicial = row['dt_admissao']
                            newDtInicial += datetime.timedelta(days=1)

                            newDtFinal = row['dt_admissao']
                            newDtFinal += datetime.timedelta(days=31)

                            dadoAlterado.append(f"Adicionado a data inicial {newDtInicial} e data final {newDtFinal} para o funcionário {row['i_funcionarios']} da turma {row['i_turmas']} da entidade {row['i_entidades']}")
                            comandoUpdate += f"UPDATE bethadba.permuta_func_turmas set dt_inicial = '{newDtInicial}' and dt_final = '{newDtFinal}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_turmas = {row['i_turmas']};\n"

                        elif row['dt_inicial'] is None:
                            newDtInicial = row['dt_admissao']
                            newDtInicial += datetime.timedelta(days=1)

                            dadoAlterado.append(f"Adicionado a data inicial {newDtInicial} para o funcionário {row['i_funcionarios']} da turma {row['i_turmas']} da entidade {row['i_entidades']}")
                            comandoUpdate += f"UPDATE bethadba.permuta_func_turmas set dt_inicial = '{newDtInicial}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_turmas = {row['i_turmas']};\n"

                        elif row['dt_final'] is None:
                            newDtFinal = row['dt_inicial']
                            newDtFinal += datetime.timedelta(days=30)

                            dadoAlterado.append(f"Adicionado a data final {newDtFinal} para o funcionário {row['i_funcionarios']} da turma {row['i_turmas']} da entidade {row['i_entidades']}")
                            comandoUpdate += f"UPDATE bethadba.permuta_func_turmas set dt_final = '{newDtFinal}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_turmas = {row['i_turmas']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_inicio_e_fim_nula: {e}")
            return

        if dt_inicio_e_fim_nula:
            dado = analisa_dt_inicio_e_fim_nula()

            if corrigirErros and len(dado) > 0:
                corrige_dt_inicio_e_fim_nula(listDados=dado)

    if dadosList:
        analisa_corrige_dt_inicio_e_fim_nula(pre_validacao="dt_inicio_e_fim_nula")
