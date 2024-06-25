from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def beneficio(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular(pre_validacao):
        nomeValidacao = "Data inicial do benefício do dependente é maior que a vigência inicial do titular."

        def analisa_func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        fps.i_entidades,
                        fps.i_funcionarios,
                        fps.i_pessoas,
                        fps.i_sequenciais,
                        vigencia_inicial AS vigencia_inicial_dependente,
                        vigencia_inicial_titular = (select max(vigencia_inicial) FROM bethadba.func_planos_saude WHERE i_sequenciais = 1 AND i_funcionarios = fps.i_funcionarios and fps.i_pessoas = i_pessoas)
                    FROM 
                        bethadba.func_planos_saude fps 
                    WHERE 
                        fps.i_sequenciais != 1
                        AND fps.vigencia_inicial < vigencia_inicial_titular
                        and fps.i_pessoas = i_pessoas   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data de vigência inicial do dependente {row['i_pessoas']} do funcionario {row['i_funcionarios']} entidade {row['i_entidades']} para {row['vigencia_inicial_titular']}")
                        comandoUpdate += f"""UPDATE bethadba.func_planos_saude set vigencia_inicial = '{row['vigencia_inicial_titular']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_pessoas = {row['i_pessoas']} and i_sequenciais = {row['i_sequenciais']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular: {e}")
            return

        if func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular:
            dado = analisa_func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular()

            if corrigirErros and len(dado) > 0:
                corrige_func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular(listDados=dado)

    if dadosList:
        analisa_corrige_func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular(pre_validacao="func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular")
