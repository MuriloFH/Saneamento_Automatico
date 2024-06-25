from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp
import datetime


def historicoPessoaFisica(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                          corrigirErros=False,
                          dt_alteracao_menor_dt_nascimento=False,
                          dt_nascimento_nulo=False,
                          cpf_invalido=False
                          ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_alteracao_menor_dt_nascimento(pre_validacao):
        nomeValidacao = "Data de alteração do historico menor que a data de nascimento"

        def analisa_dt_alteracao_menor_dt_nascimento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_alteracao_menor_dt_nascimento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select hist.i_pessoas,hist.dt_alteracoes, hist.dt_nascimento, pf.dt_nascimento as pfDtNascimento
                                            from bethadba.hist_pessoas_fis hist
                                            join bethadba.pessoas_fisicas pf on (pf.i_pessoas = hist.i_pessoas)
                                            where hist.dt_nascimento > hist.dt_alteracoes
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        print(row)
                        if row['dt_alteracoes'] > row['pfDtNascimento']:
                            newDtNascimentoHist = row['pfDtNascimento']
                        else:
                            newDtNascimentoHist = row['dt_alteracoes']
                            newDtNascimentoHist -= datetime.timedelta(days=30)

                        dadoAlterado.append(f"Alterado a data de nascimento no historico da pessoa {row['i_pessoas']}")
                        comandoUpdate += f"UPDATE bethadba.hist_pessoas_fis set dt_nascimento = '{newDtNascimentoHist}' where i_pessoas = {row['i_pessoas']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_alteracao_menor_dt_nascimento: {e}")
            return

        if dt_alteracao_menor_dt_nascimento:
            dado = analisa_dt_alteracao_menor_dt_nascimento()

            if corrigirErros and len(dado) > 0:
                corrige_dt_alteracao_menor_dt_nascimento(listDados=dado)

    def analisa_corrige_dt_nascimento_nulo(pre_validacao):
        nomeValidacao = "Data de nascimento vazio"

        def analisa_dt_nascimento_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_nascimento_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select hpf.i_pessoas, pf.dt_nascimento as dtNascimentoPf, hpf.dt_alteracoes
                                            from bethadba.hist_pessoas_fis hpf
                                            left join bethadba.pessoas_fisicas pf on (hpf.i_pessoas = pf.i_pessoas)
                                            where hpf.dt_nascimento is null
                                            order by hpf.i_pessoas, hpf.dt_alteracoes
                                        """)

                for row in dados:
                    dadoAlterado.append(f"Inserido a data de nascimento no historico com data de {row['dt_alteracoes']} da pessoa {row['i_pessoas']}")
                    comandoUpdate += f"""UPDATE bethadba.hist_pessoas_fis set dt_nascimento = '{row['dtNascimentoPf']}' where i_pessoas = {row['i_pessoas']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_nascimento_nulo: {e}")
            return

        if dt_nascimento_nulo:
            dado = analisa_dt_nascimento_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_dt_nascimento_nulo(listDados=dado)

    def analisa_corrige_cpf_invalido(pre_validacao):
        nomeValidacao = "CPF inválido."

        def analisa_cpf_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cpf_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select hpf.i_pessoas ,
                                             hpf.dt_alteracoes ,
                                             pf.cpf as pfCPF,
                                             isnumeric(pf.cpf) validaCpfPf,
                                             hpf.cpf as hpfCPF,
                                             isnumeric(hpf.cpf) as validacao 
                                             from bethadba.hist_pessoas_fis hpf
                                             left join bethadba.pessoas_fisicas pf on (hpf.i_pessoas = pf.i_pessoas)
                                             where hpf.cpf is not null and hpf.cpf <> '' and validacao = 0
                                        """)

                for row in dados:
                    print(row)
                    if row['pfCPF'] is not None and row['validaCpfPf'] == 1:
                        newCpfHist = row['pfCPF']
                    else:
                        newCpfHist = geraCfp()

                    dadoAlterado.append(f"Inserido o cpf {newCpfHist} no historico com data de {row['dt_alteracoes']} da pessoa {row['i_pessoas']}")
                    comandoUpdate += f"""UPDATE bethadba.hist_pessoas_fis set cpf = '{newCpfHist}' where i_pessoas = {row['i_pessoas']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cpf_invalido: {e}")
            return

        if cpf_invalido:
            dado = analisa_cpf_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_invalido(listDados=dado)

    if dadosList:
        analisa_corrige_dt_alteracao_menor_dt_nascimento(pre_validacao="dt_alteracao_menor_dt_nascimento")
        analisa_corrige_dt_nascimento_nulo(pre_validacao='dt_nascimento_nulo')
        analisa_corrige_cpf_invalido(pre_validacao='cpf_invalido')
