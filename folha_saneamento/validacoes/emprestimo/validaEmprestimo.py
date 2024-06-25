from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def emprestimo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
               corrigirErros=False,
               dt_inicio_emprestimo_menor_dt_admissao=False,
               ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_inicio_emprestimo_menor_dt_admissao(pre_validacao):
        nomeValidacao = "Data de inicio do empréstimo inferior a data de admissão"

        def analisa_dt_inicio_emprestimo_menor_dt_admissao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_inicio_emprestimo_menor_dt_admissao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select funcionarios.i_entidades,funcionarios.i_funcionarios, funcionarios.i_pessoas, dt_admissao, i_emprestimos
                                            from bethadba.emprestimos,
                                                 bethadba.funcionarios
                                            where funcionarios.i_entidades = emprestimos.i_entidades and 
                                                  funcionarios.i_funcionarios = emprestimos.i_funcionarios and
                                                  dt_admissao> dt_emprestimo
                                        """)

                for row in dados:
                    newDtEmprestimo = row['dt_admissao']

                    dadoAlterado.append(f"""Alterado a data inicial do empréstimo {row['i_emprestimos']} para {newDtEmprestimo} do funcionário {row['i_entidades']} da entidade {row['i_entidades']}""")
                    comandoUpdate += f"""UPDATE bethadba.emprestimos set dt_emprestimo = '{newDtEmprestimo}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_emprestimos = {row['i_emprestimos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_inicio_emprestimo_menor_dt_admissao: {e}")
            return

        if dt_inicio_emprestimo_menor_dt_admissao:
            dado = analisa_dt_inicio_emprestimo_menor_dt_admissao()

            if corrigirErros and len(dado) > 0:
                corrige_dt_inicio_emprestimo_menor_dt_admissao(listDados=dado)

    if dadosList:
        analisa_corrige_dt_inicio_emprestimo_menor_dt_admissao(pre_validacao="dt_inicio_emprestimo_menor_dt_admissao")
