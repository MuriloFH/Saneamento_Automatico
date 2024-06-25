from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def processoTrabalhista(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        dt_homologacao_invalida=False
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_homologacao_invalida(pre_validacao):
        nomeValidacao = "Data de homologação do processo maior que a data fim ou data atual"

        def analisa_dt_homologacao_invalida():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_homologacao_invalida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                funcAtual = None
                entAtual = None
                for func in listDados:
                    if entAtual != func['i_chave_dsk1']:
                        entAtual = func['i_chave_dsk1']
                        if funcAtual != func['i_chave_dsk2']:
                            funcAtual = func['i_chave_dsk2']
                            busca = banco.consultar(f"""SELECT *
                                                        from bethadba.periodos p
                                                        where p.i_entidades = {entAtual} and p.i_funcionarios = {funcAtual}
                                                        order by i_periodos desc
                                                    """)
                            newDtInicial = ""
                            newDtFinal = ""
                            newDtFinallUltimoPeriodo = ""
                            anoDtInicial = ""
                            mesDiaDtInicial = ""

                            if len(busca) > 0:
                                for i in range(0, len(busca)):
                                    # considerando a data do ultimo periodo correta
                                    if i == 0:
                                        newDtFinallUltimoPeriodo = busca[i]['dt_aquis_fin']

                                        newDtInicial = newDtFinallUltimoPeriodo
                                        newDtInicial -= datetime.timedelta(days=364)
                                        calcDtInicial = newDtInicial

                                        mesDiaDtInicial = str(calcDtInicial)[5:]
                                        anoDtInicial = str(calcDtInicial)[:4]

                                        dadoAlterado.append(f"Alterado a data inicial para {newDtInicial} e final para {newDtFinallUltimoPeriodo} do periodo {busca[i]['i_periodos']} do funcionario {funcAtual} da entidade {entAtual}")
                                        comandoUpdate += f"""UPDATE bethadba.periodos set dt_aquis_ini = '{newDtInicial}', dt_aquis_fin = '{newDtFinallUltimoPeriodo}' 
                                                             where i_entidades = {entAtual} and i_funcionarios = {funcAtual} and i_periodos = {busca[i]['i_periodos']};\n"""
                                        continue

                                    newDtFinal = newDtInicial
                                    newDtFinal -= datetime.timedelta(days=1)

                                    newAno = (int(anoDtInicial) - i)

                                    newDtInicial = f'{newAno}-{mesDiaDtInicial}'
                                    newDtInicial = datetime.datetime.strptime(newDtInicial, "%Y-%m-%d").date()

                                    dadoAlterado.append(f"Alterado a data inicial para {newDtInicial} e final para {newDtFinal} do periodo {busca[i]['i_periodos']} do funcionario {funcAtual} da entidade {entAtual}")
                                    comandoUpdate += f"""UPDATE bethadba.periodos set dt_aquis_ini = '{newDtInicial}', dt_aquis_fin = '{newDtFinal}' 
                                                         where i_entidades = {entAtual} and i_funcionarios = {funcAtual} and i_periodos = {busca[i]['i_periodos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_periodo_dt_homologacao_invalida: {e}")
            return

        if dt_homologacao_invalida:
            dado = analisa_dt_homologacao_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_dt_homologacao_invalida(listDados=dado)

    if dadosList:
        analisa_corrige_dt_homologacao_invalida(pre_validacao="dt_homologacao_invalida")
