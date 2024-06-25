from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp
import polars as pl


def processoAdmParticipante(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                            corrigirErros=False,
                            representante_participante_cpf_invalido=False,
                            participante_nao_convidado_para_processo=False
                            ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_representante_participante_cpf_invalido(pre_validacao):
        nomeValidacao = "O Representante do Participante possui CPF invalido."

        def analisa_representante_participante_cpf_invalido():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_representante_participante_cpf_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    SELECT  
                        i_ano_proc,
                        i_processo,
                        i_credores,
                        compras.dbf_valida_cgc_cpf(null,cpf_representante,'F','S') as valida_cpf,
                        cpf_representante,
                        i_entidades
                    FROM compras.participantes_processos
                    WHERE valida_cpf = 0
                    and cpf_representante is not null
                """)
                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df = df.with_columns(
                        pl.col('i_credores').map_elements(lambda x: geraCfp()).alias('novo_cpf')
                    )

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado CPF do representante  {row['i_credores']} do processo {row['i_processo']}/{row['i_ano_proc']} para {row['novo_cpf']}")
                        comandoUpdate += f"""UPDATE compras.participantes_processos set cpf_representante = '{row['novo_cpf']}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_representante_participante_cpf_invalido: {e}")
            return

        if representante_participante_cpf_invalido:
            dado = analisa_representante_participante_cpf_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_representante_participante_cpf_invalido(listDados=dado)

    def analisa_corrige_participante_nao_convidado_para_processo(pre_validacao):
        nomeValidacao = "O participante não está convidado para o processo modalidade convite é necessário que o participante esteja convidado para o processo."

        def analisa_participante_nao_convidado_para_processo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_participante_nao_convidado_para_processo(listDados):
            tipoCorrecao = "INSERCAO"
            comandoInsert = ""
            dadoInserido = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                       SELECT 
                            p.i_processo,
                            p.i_ano_proc,
                            i_credores,
                            pp.modalidade,
                            pp.data_processo,
                            p.i_entidades 
                        FROM compras.participantes p
                        INNER JOIN compras.processos pp ON pp.i_ano_proc = p.i_ano_proc AND pp.i_processo = p.i_processo AND pp.i_entidades = p.i_entidades
                        WHERE pp.modalidade IN (1,2)
                        AND NOT EXISTS (SELECT 1 FROM compras.convidados c	WHERE c.i_credores = p.i_credores and c.i_entidades = p.i_entidades)
                        UNION
                        SELECT 
                            ppro.i_processo,
                            ppro.i_ano_proc,
                            i_credores,
                            pp.modalidade,
                            pp.data_processo,
                            pp.i_entidades 
                        FROM compras.participantes_processos ppro
                        INNER JOIN compras.processos pp ON pp.i_ano_proc = ppro.i_ano_proc AND pp.i_processo = ppro.i_processo AND pp.i_entidades = ppro.i_entidades
                        WHERE pp.modalidade IN (1, 2)
                        AND NOT EXISTS (SELECT 1 FROM compras.convidados c	WHERE c.i_credores = ppro.i_credores and c.i_entidades = ppro.i_entidades)
                   """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoInserido.append(f"Inserido convidado  {row['i_credores']} na tabela compras.convidados ref ao processo {row['i_processo']}/{row['i_ano_proc']}")
                        comandoInsert += f"""INSERT INTO compras.convidados(i_ano_proc, i_processo, i_credores, nr_protocolo, data_protocolo, auto_convocou, data_recibo, obs_convite, i_entidades) VALUES({row['i_ano_proc']}, {row['i_processo']}, {row['i_credores']}, NULL, '{row['data_processo']}', 'N', '{row['data_processo']}', NULL, {row['i_entidades']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro,
                                     preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_participante_nao_convidado_para_processo: {e}")
            return

        if participante_nao_convidado_para_processo:
            dado = analisa_participante_nao_convidado_para_processo()

            if corrigirErros and len(dado) > 0:
                corrige_participante_nao_convidado_para_processo(listDados=dado)

    if dadosList:
        analisa_corrige_representante_participante_cpf_invalido(pre_validacao="representante_participante_cpf_invalido")
        analisa_corrige_participante_nao_convidado_para_processo(pre_validacao="participante_nao_convidado_para_processo")
