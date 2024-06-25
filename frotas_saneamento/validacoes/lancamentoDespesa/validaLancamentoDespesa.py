from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime as dt
import polars
from utilitarios.funcoesGenericas.funcoes import remove_caracteres_especiais


def lancamentoDespesa(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                      corrigirErros=False,
                      despesa_motorista_nulo=False,
                      caractere_especial_documento=False,
                      numero_documento_nulo=False,
                      kilometragem_despesa_inferior_despesa_anterior=False,
                      mudanca_odometro_sem_lancamento=False
                      ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    tipoValidacao = tipo_registro

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_despesa_motorista_nulo(pre_validacao):
        nomeValidacao = "Despesa sem motorista informado"
        preValidacaoBanco = pre_validacao

        def analisa_despesa_motorista_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_despesa_motorista_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                newFuncionario = banco.consultar("""select first i_funcionarios as func from bethadba.funcionarios""")

                try:
                    for dados in listDados:
                        dadoAlterado.append(f"Adicionado o funcionario {newFuncionario[0]['func']} no lançamento de despesa {dados['i_chave_dsk1']}")

                    comandoUpdate += f"update bethadba.movimentos set i_funcionarios = {newFuncionario[0]['func']} where i_funcionarios is null;\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)
                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_despesa_motorista_nulo {e}")

        if despesa_motorista_nulo:
            dado = analisa_despesa_motorista_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_despesa_motorista_nulo(listDados=dado)

    def valida_corrige_caractere_especial_documento(pre_validacao):
        nomeValidacao = "Caracteres texto no campo de número do documento"
        preValidacaoBanco = pre_validacao

        def analisa_caractere_especial_documento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_caractere_especial_documento(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""SELECT i_movimentos, nota from bethadba.movimentos m where m.i_movimentos = {dados['i_chave_dsk1']}""")

                        newNumeroDocumento = remove_caracteres_especiais(busca[0]['nota'])

                        dadoAlterado.append(f"Alterado o numero da nota {busca[0]['nota']} para {newNumeroDocumento} no lançamento de despesa {busca[0]['i_movimentos']}")
                        comandoUpdate += f"update bethadba.movimentos set nota = {newNumeroDocumento} where i_movimentos = {busca[0]['i_movimentos']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_caractere_especial_documento {e}")

        if caractere_especial_documento:
            dado = analisa_caractere_especial_documento()

            if corrigirErros and len(dado) > 0:
                corrige_caractere_especial_documento(listDados=dado)

    def valida_corrige_numero_documento_nulo(pre_validacao):
        nomeValidacao = "Caracteres texto no campo de número do documento"
        preValidacaoBanco = pre_validacao

        def analisa_numero_documento_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_numero_documento_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        newNumeroDocumento = "0"

                        dadoAlterado.append(f"Adicionado o numero da nota {newNumeroDocumento} no lançamento de despesa {dados['i_chave_dsk1']}")
                        comandoUpdate += f"update bethadba.movimentos set nota = {newNumeroDocumento} where i_movimentos = {dados['i_chave_dsk1']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_numero_documento_nulo {e}")

        if numero_documento_nulo:
            dado = analisa_numero_documento_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_numero_documento_nulo(listDados=dado)

    def valida_corrige_kilometragem_despesa_inferior_despesa_anterior(pre_validacao):
        nomeValidacao = "Kilometragem inferior das despesas anteriores"
        preValidacaoBanco = pre_validacao

        def analisa_kilometragem_despesa_inferior_despesa_anterior():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_kilometragem_despesa_inferior_despesa_anterior(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    cont = 0
                    df = polars.DataFrame(listDados)
                    dtLimpo = df.unique(subset=['i_chave_dsk1'])
                    listDadosLimpo = dtLimpo.to_dict()

                    for dados in listDadosLimpo['i_chave_dsk1']:
                        comandoUpdate = ""
                        dadoAlterado = []
                        listKilometragem = []
                        segundos = 0
                        busca = banco.consultar(f"""
                                                    select v.i_seq_veiculo as codigo, m.i_veiculos, m.i_movimentos, m.kilom, m.data_mov
                                                    from bethadba.movimentos m key join bethadba.veiculos v
                                                    where m.i_veiculos = '{dados}' and m.inf_km = 'S'
                                                    order by m.i_veiculos, m.data_mov, i_movimentos
                                                    """)

                        for i in busca:
                            listKilometragem.append(i['kilom'])

                        listKilometragem = sorted(listKilometragem)

                        for j in range(0, len(listKilometragem)):
                            busca[j]['kilom'] = listKilometragem[j]
                            busca[j]['data_mov'] += dt.timedelta(seconds=segundos)

                            segundos += 1

                            comandoUpdate += f"UPDATE bethadba.movimentos set kilom = {busca[j]['kilom']}, data_mov = '{busca[j]['data_mov']}' where i_veiculos = '{busca[j]['i_veiculos']}' and i_movimentos = {busca[j]['i_movimentos']};\n"

                        dadoAlterado.append(f"Ordenado a kilometragem da despesa {busca[0]['i_movimentos']}")

                        banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função corrige_kilometragem_despesa_inferior_despesa_anterior {e}")

        if kilometragem_despesa_inferior_despesa_anterior:
            dado = analisa_kilometragem_despesa_inferior_despesa_anterior()

            if corrigirErros and len(dado) > 0:
                corrige_kilometragem_despesa_inferior_despesa_anterior(listDados=dado)

    def valida_corrige_mudanca_odometro_sem_lancamento(pre_validacao):
        nomeValidacao = "Alteração de odometro sem lançamento informado"
        preValidacaoBanco = pre_validacao

        def analisa_mudanca_odometro_sem_lancamento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_mudanca_odometro_sem_lancamento(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
                dadoAlterado = []
                comandoInsert = ""

                try:
                    busca = banco.consultar(f"""SELECT i_veiculos AS i_veiculos,
                                                    first_i_mov AS first_i_mov,
                                                    kilom AS kilom,
                                                    CAST(data_mov AS date) AS data_mov,
                                                    odometro AS odometro,
                                                    entidade
                                                FROM (SELECT odometro_atual AS odometro,
                                                        i_veiculos,
                                                        first_i_mov,
                                                        odometro_original,
                                                     (SELECT FIRST i_entidades
                                                        from bethadba.movimentos
                                                        where i_movimentos = first_i_mov) as entidade,
                                                    (SELECT kilom
                                                    FROM bethadba.movimentos
                                                    WHERE first_i_mov = i_movimentos) AS kilom,
                                                    (SELECT data_mov
                                                    FROM bethadba.movimentos
                                                    WHERE first_i_mov = i_movimentos) AS data_mov,
                                                        isnull((SELECT 1
                                                                FROM bethadba.lanc_ocorr AS lo
                                                                WHERE first_i_mov = i_movimentos), 0) AS possui_ocorrencia
                                                FROM (SELECT DISTINCT odometro AS odometro_atual,

                                                        (SELECT min(odometro)
                                                        FROM bethadba.movimentos AS mo
                                                        WHERE mo.i_veiculos = m.i_veiculos) AS odometro_original,
                                                        i_veiculos,
                                                        (SELECT TOP 1 i_movimentos
                                                        FROM bethadba.movimentos AS mq
                                                        WHERE mq.odometro = m.odometro
                                                        AND mq.i_veiculos = m.i_veiculos
                                                        ORDER BY data_mov ASC) AS first_i_mov
                                                    FROM bethadba.movimentos AS m
                                                    WHERE m.inf_km = 'S'
                                                        AND odometro_atual > odometro_original) AS pm
                                                WHERE possui_ocorrencia = 0
                                                    AND (year(data_mov) BETWEEN 1900 AND (year(now())+1)) ) AS novo_odometro_sem_ocorrencia
                                                """)

                    ocorrencia = banco.consultar(f"""SELECT * from bethadba.ocorrencias o where o.adaptacao = 'S'""")
                    if len(ocorrencia) == 0:
                        ocorrencia = 'null'
                    else:
                        ocorrencia = ocorrencia[0]['i_ocorrencias']

                    lastLancamento = banco.consultar(f"""SELECT max(i_lanc_ocorr) as lancamento from bethadba.lanc_ocorr lo """)[0]['lancamento']

                    for row in busca:
                        lastLancamento += 1
                        dadoAlterado.append(f"Inserido o lançamento para a movimentação {row['first_i_mov']}.")
                        comandoInsert += f"""INSERT into bethadba.lanc_ocorr (i_lanc_ocorr, i_entidades, data, i_ocorrencias, i_veiculos, odometro, km, i_movimentos, obs)
                                                values({lastLancamento}, {row['entidade']}, '{row['data_mov']}', {ocorrencia}, '{row['i_veiculos']}', {row['odometro']}, {row['kilom']}, {row['first_i_mov']}, 'Correção saneamento - LancamentoDespesa');\n"""

                    banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_mudanca_odometro_sem_lancamento {e}")

        if mudanca_odometro_sem_lancamento:
            dado = analisa_mudanca_odometro_sem_lancamento()

            if corrigirErros and len(dado) > 0:
                corrige_mudanca_odometro_sem_lancamento(listDados=dado)

    if dadosList:
        valida_corrige_despesa_motorista_nulo(pre_validacao='despesa_motorista_nulo')
        valida_corrige_caractere_especial_documento(pre_validacao='caractere_especial_documento')
        valida_corrige_numero_documento_nulo(pre_validacao='numero_documento_nulo')
        valida_corrige_kilometragem_despesa_inferior_despesa_anterior(pre_validacao='kilometragem_despesa_inferior_despesa_anterior')
        valida_corrige_mudanca_odometro_sem_lancamento(pre_validacao='mudanca_odometro_sem_lancamento')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
