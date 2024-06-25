from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from calendar import monthrange
import datetime


def dataFechamentoFolha(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        dt_fechamento_nulo=False
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_fechamento_nulo(pre_validacao):
        nomeValidacao = "Data de fechamento do calculo da folha vazio"

        def analisa_dt_fechamento_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_fechamento_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select funcionarios.i_funcionarios, dados_calc.i_tipos_proc, dados_calc.i_processamentos, dados_calc.i_entidades, dados_calc.i_competencias, dados_calc.dt_fechamento
                                            from bethadba.dados_calc, bethadba.funcionarios 
                                            where dados_calc.i_entidades = funcionarios.i_entidades
                                            and dados_calc.i_funcionarios = funcionarios.i_funcionarios 
                                            and dt_fechamento is null
                                            order by dados_calc.i_competencias desc
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        competenciaCalc = row['i_competencias']
                        newDtFechamento = competenciaCalc.replace(day=monthrange(competenciaCalc.year, competenciaCalc.month)[1])
                        newDtFechamento -= datetime.timedelta(days=2)

                        dadoAlterado.append(f"Adicionado a data de fechamento {newDtFechamento} para o funcionário {row['i_funcionarios']} na competência {row['i_competencias']} do processamento {row['i_processamentos']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.dados_calc set dt_fechamento = '{newDtFechamento}'
                                            where i_funcionarios = {row['i_funcionarios']}
                                            and i_competencias = '{row['i_competencias']}'
                                            and i_tipos_proc = {row['i_tipos_proc']}
                                            and i_processamentos = {row['i_processamentos']}
                                            and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_fechamento_nulo: {e}")
            return

        if dt_fechamento_nulo:
            dado = analisa_dt_fechamento_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_dt_fechamento_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_dt_fechamento_nulo(pre_validacao="dt_fechamento_nulo")
