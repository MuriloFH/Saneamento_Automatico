from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def horarioPonto(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 descricao_repetida=False,
                 descricao_repetida_turmas=False,
                 descricao_motivo_altera_ponto_maior_30_caracter=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_descricao_repetida(pre_validacao):
        nomeValidacao = "Descrição de horario ponto repetidas"

        def analisa_descricao_repetida():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_descricao_repetida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT 
                                                list(i_entidades) as entidades, 
                                                list(i_horarios_ponto) as horario, 
                                                trim(descricao) as descricao,
                                                count(descricao) AS quantidade 
                                            FROM 
                                                bethadba.horarios_ponto
                                            GROUP BY 
                                                descricao 
                                            HAVING
                                                quantidade > 1
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        entidades = row['entidades'].split(',')
                        horarios = row['horario'].split(',')

                        for i in range(0, len(horarios)):
                            codigoDescricao = f"{horarios[i]}{entidades[i]}{i}"
                            newDescricao = f"{row['descricao']} | {codigoDescricao}"
                            if len(newDescricao) > 45:
                                newDescricao = f"{row['descricao'][:(45 - len(codigoDescricao) * 2)]} | {codigoDescricao}"

                            dadoAlterado.append(f"Alterado a descrição do horario {horarios[i]} para {newDescricao} da entidade {entidades[i]}")
                            comandoUpdate += f"UPDATE bethadba.horarios_ponto set descricao = '{newDescricao}' where i_entidades = {entidades[i]} and i_horarios_ponto = {horarios[i]};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_repetida: {e}")
            return

        if descricao_repetida:
            dado = analisa_descricao_repetida()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_repetida(listDados=dado)

    def analisa_corrige_descricao_repetida_turmas(pre_validacao):
        nomeValidacao = "Descrição repetidas de horario ponto das turmas"

        def analisa_descricao_repetida_turmas():
            print(f">> Iniciando a validação '{nomeValidacao}'")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_descricao_repetida_turmas(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT 
                                                list(i_entidades) as entidades, 
                                                list(i_turmas) as turma, 
                                                trim(descricao) descricao,
                                                count(descricao) AS quantidade 
                                            FROM 
                                                bethadba.turmas    
                                            GROUP BY 
                                                descricao 
                                            HAVING
                                                quantidade > 1
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        entidades = row['entidades'].split(',')
                        turmas = row['turma'].split(',')

                        for i in range(0, len(turmas)):
                            codigoDescricao = f"{turmas[i]}{entidades[i]}{i}"
                            newDescricao = f"{row['descricao']} | {codigoDescricao}"
                            if len(newDescricao) > 45:
                                newDescricao = f"{row['descricao'][:(45 - len(codigoDescricao) * 2)]} | {codigoDescricao}"

                            dadoAlterado.append(f"Alterado a descrição do horario da turma {turmas[i]} para {newDescricao} da entidade {entidades[i]}")
                            comandoUpdate += f"UPDATE bethadba.turmas set descricao = '{newDescricao}' where i_entidades = {entidades[i]} and i_turmas = {turmas[i]};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_repetida_turmas: {e}")
            return

        if descricao_repetida_turmas:
            dado = analisa_descricao_repetida_turmas()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_repetida_turmas(listDados=dado)

    def analisa_corrige_descricao_motivo_altera_ponto_maior_30_caracter(pre_validacao):
        nomeValidacao = "Descrição do motivo de alteração de ponto maior que 30 caracter"

        def analisa_descricao_motivo_altera_ponto_maior_30_caracter():
            print(f">> Iniciando a validação '{nomeValidacao}'")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_descricao_motivo_altera_ponto_maior_30_caracter(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT
                                                i_motivos_altponto,
                                                descricao,
                                                LENGTH(descricao) AS tamanho_descricao
                                            FROM
                                                bethadba.motivos_altponto 
                                            WHERE 
                                                tamanho_descricao > 30
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newDescricao = row['descricao'][:30]
                        dadoAlterado.append(f"Alterado a descrição do motivo {row['i_motivos_altponto']} para {newDescricao}")
                        comandoUpdate += f"UPDATE bethadba.motivos_altponto set descricao = '{newDescricao}' where motivos_altponto.i_motivos_altponto = {row['i_motivos_altponto']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_motivo_altera_ponto_maior_30_caracter: {e}")
            return

        if descricao_motivo_altera_ponto_maior_30_caracter:
            dado = analisa_descricao_motivo_altera_ponto_maior_30_caracter()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_motivo_altera_ponto_maior_30_caracter(listDados=dado)

    if dadosList:
        analisa_corrige_descricao_repetida(pre_validacao="descricao_repetida")
        analisa_corrige_descricao_repetida_turmas(pre_validacao='descricao_repetida_turmas')
        analisa_corrige_descricao_motivo_altera_ponto_maior_30_caracter(pre_validacao='descricao_motivo_altera_ponto_maior_30_caracter')
