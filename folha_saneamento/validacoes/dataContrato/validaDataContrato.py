from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def dataContrato(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 pensionista_dt_admissao_menor_dt_rescisao_instituidor=False,
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_pensionista_dt_admissao_menor_dt_rescisao_instituidor(pre_validacao):
        nomeValidacao = "Pensionista com data menor que rescisão da matricula de origem com pensão por morte"

        def analisa_pensionista_dt_admissao_menor_dt_rescisao_instituidor():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pensionista_dt_admissao_menor_dt_rescisao_instituidor(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select b.i_entidades as entidade,
                                            b.i_funcionarios as beneficiario,
                                            b.i_instituidor as instituidor,
                                            f.dt_admissao as admiBeneficiario,
                                            r.dt_rescisao as resInstituidor
                                            from bethadba.beneficiarios b, bethadba.rescisoes r, bethadba.funcionarios f
                                            where b.i_entidades = r.i_entidades 
                                            and b.i_instituidor = r.i_funcionarios
                                            and b.i_funcionarios = f.i_funcionarios 
                                            and b.i_entidades = f.i_entidades
                                            and f.tipo_pens = 1 
                                            and r.trab_dia_resc = 'S'
                                            and f.dt_admissao <= r.dt_rescisao
                                            order by entidade, beneficiario
                                        """)

                for row in dados:
                    newDtAdmissao = row['resInstituidor']
                    newDtAdmissao += datetime.timedelta(days=1)

                    dadoAlterado.append(f"Alterado a data de admissão para {newDtAdmissao} do beneficiário {row['beneficiario']} da entidade {row['entidade']}")
                    comandoUpdate += f"""UPDATE bethadba.funcionarios set dt_admissao = '{newDtAdmissao}' where i_entidades = {row['entidade']} and i_funcionarios = {row['beneficiario']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pensionista_dt_admissao_menor_dt_rescisao_instituidor: {e}")
            return

        if pensionista_dt_admissao_menor_dt_rescisao_instituidor:
            dado = analisa_pensionista_dt_admissao_menor_dt_rescisao_instituidor()

            if corrigirErros and len(dado) > 0:
                corrige_pensionista_dt_admissao_menor_dt_rescisao_instituidor(listDados=dado)

    if dadosList:
        analisa_corrige_pensionista_dt_admissao_menor_dt_rescisao_instituidor(pre_validacao="pensionista_dt_admissao_menor_dt_rescisao_instituidor")
