from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp, geraCnpj, geraInscricaoEstadual, generateInscricaoMunicipal
import pandas as pd


def fornecedores(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 cnpj_cpf_invalido=False,
                 cnpj_cpf_nulo=False,
                 fornecedor_estado_nulo=False,
                 cnpj_cpf_duplicado=False,
                 inscricao_estadual_duplicada=False,
                 inscricao_municipal_duplicada=False,
                 data_situacao_maior_data_atual=False):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def alteraTipoFornecedor():
        list_i_fornec = []
        busca = banco.consultar(f"SELECT f.i_fornecedor from bethadba.fornecedores f where f.tipo in ('o', 'O')")
        if len(busca) > 0:
            for i in busca:
                list_i_fornec.append(i['i_fornecedor'])

            list_i_fornec = ', '.join((str(id) for id in list_i_fornec))

            comandoUpdate = f"UPDATE bethadba.fornecedores set tipo = 'J' where i_fornecedor in ({list_i_fornec});"
            banco.executar(comando=banco.triggerOff(comandoUpdate))
        return

    def analisa_corrige_cnpj_cpf_invalido(pre_validacao):
        nomeValidacao = "Fornecedor com CNPJ ou CPF inválidos"

        def analisa_cnpj_cpf_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cnpj_cpf_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            alteraTipoFornecedor()  # chamando a função, pois caso tenha algum fornecedor com o tipo diferente de F ou J, o fornecedor será definido como juridico.

            try:
                busca = banco.consultar(f"""select 
                                                f.i_fornecedor,
                                                f.tipo,
                                                f.cpf as cnpj_cpf,
                                                bethadba.dbf_valida_cgc_cpf(null,f.cpf,'F','S') as valida_cnpj_cpf
                                            from bethadba.fornecedores f 
                                            where f.tipo = 'F' and valida_cnpj_cpf = 0
                                            and f.i_fornecedor in (select distinct entradas.i_fornecedor
                                                                      from bethadba.entradas
                                                                     where entradas.i_fornecedor is not null)
                                            union
                                            select 
                                                f.i_fornecedor,
                                                f.tipo,
                                                f.cnpj as cnpj_cpf,
                                                bethadba.dbf_valida_cgc_cpf(f.cnpj,null,'J','S') as valida_cnpj_cpf
                                            from bethadba.fornecedores f 
                                            where f.tipo = 'J' and valida_cnpj_cpf = 0
                                            and f.i_fornecedor in (select distinct entradas.i_fornecedor
                                                                      from bethadba.entradas
                                                                     where entradas.i_fornecedor is not null)  
                """)

                if len(busca) > 0:
                    df = pd.DataFrame(busca)
                    df['novo_documento'] = df.apply(lambda row: geraCfp() if row['tipo'] == 'F' else geraCnpj(), axis=1)

                    for row in df.itertuples():
                        match row.tipo:
                            case "J":
                                dadoAlterado.append(f"Adicionado o CNPJ {row.novo_documento} para o Fornecedor {row.i_fornecedor}")
                                comandoUpdate += f"""UPDATE bethadba.fornecedores set cnpj = '{row.novo_documento}' where i_fornecedor = {row.i_fornecedor};\n"""
                            case "F":
                                dadoAlterado.append(f"Adicionado o CPF {row.novo_documento} para o Fornecedor {row.i_fornecedor}")
                                comandoUpdate += f"""UPDATE bethadba.fornecedores set cpf = '{row.novo_documento}' where i_fornecedor = {row.i_fornecedor};\n"""
                            case _:
                                comandoUpdate += ""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_cpf_invalido: {e}")
            return

        if cnpj_cpf_invalido:
            dado = analisa_cnpj_cpf_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_cpf_invalido(listDados=dado)

    def analisa_corrige_cnpj_cpf_nulo(pre_validacao):
        nomeValidacao = "Fornecedor sem CNPJ ou CPF informado"

        def analisa_cnpj_cpf_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = banco.consultar(f"""
                                    select 
                                        f.i_fornecedor,
                                        f.tipo
                                    from bethadba.fornecedores f 
                                    where (f.cnpj is null or f.cnpj = '') and (f.cpf is null or f.cpf = '')
                                    and f.i_fornecedor in (select distinct entradas.i_fornecedor
                                                                  from bethadba.entradas
                                                                 where entradas.i_fornecedor is not null)   
                                    """)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            return dados

        def corrige_cnpj_cpf_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            alteraTipoFornecedor()  # chamando a função, pois caso tenha algum fornecedor com o tipo diferente de F ou J, o fornecedor será definido como juridico.

            try:
                busca = banco.consultar(f"""
                                        select 
                                            f.i_fornecedor,
                                            f.tipo
                                        from bethadba.fornecedores f 
                                        where (f.cnpj is null or f.cnpj = '') and (f.cpf is null or f.cpf = '')
                                        and f.i_fornecedor in (select distinct entradas.i_fornecedor
                                                                      from bethadba.entradas
                                                                     where entradas.i_fornecedor is not null)   
                                        """)

                df = pd.DataFrame(busca)
                df['novo_documento'] = df.apply(lambda row: geraCfp() if row['tipo'] == 'F' else geraCnpj(), axis=1)

                for row in df.itertuples():
                    match row.tipo:
                        case "J":
                            dadoAlterado.append(f"Adicionado o CNPJ {row.novo_documento} para o Fornecedor {row.i_fornecedor}")
                            comandoUpdate += f"""UPDATE bethadba.fornecedores set cnpj = '{row.novo_documento}' where i_fornecedor = {row.i_fornecedor};\n"""
                        case "F":
                            dadoAlterado.append(f"Adicionado o CPF {row.novo_documento} para o Fornecedor {row.i_fornecedor}")
                            comandoUpdate += f"""UPDATE bethadba.fornecedores set cpf = '{row.novo_documento}' where i_fornecedor = {row.i_fornecedor};\n"""
                        case _:
                            comandoUpdate += ""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_cpf_nulo: {e}")
            return

        if cnpj_cpf_nulo:
            dado = analisa_cnpj_cpf_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_cpf_nulo(listDados=dado)

    def analisa_corrige_fornecedor_estado_nulo(pre_validacao):
        nomeValidacao = "Fornecedor sem Estado(UF) informada"

        def analisa_fornecedor_estado_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fornecedor_estado_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                                    select 
                                        f.i_fornecedor,
                                        f.estado,
                                        f.i_entidades,
                                        p.estado as estado_entidade
                                    from bethadba.fornecedores f 
                                    join bethadba.parametros p on p.i_entidades = f.i_entidades 
                                    where (f.i_cidades is null or f.i_cidades = '')
                                    and (f.estado is null or f.estado = '')
                """)

                if len(busca) > 0:
                    df = pd.DataFrame(busca)
                    for row in df.itertuples():
                        dadoAlterado.append(f"Inserido o Estado {row.estado_entidade} no fornecedor {row.i_fornecedor}")
                        comandoUpdate += f"""UPDATE bethadba.fornecedores set estado = '{row.estado_entidade}' where i_fornecedor = {row.i_fornecedor};\n"""

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_estado_nulo: {e}")
            return

        if fornecedor_estado_nulo:
            dado = analisa_fornecedor_estado_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_estado_nulo(listDados=dado)

    def analisa_corrige_cnpj_cpf_duplicado(pre_validacao):
        nomeValidacao = "Fornecedor com CNPJ ou CPF duplicados"

        def analisa_cnpj_cpf_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cnpj_cpf_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                                        SELECT 
                                            list(f.i_fornecedor) as i_fornecedor,
                                            f.tipo,
                                            case f.tipo
                                             when 'F' then trim(ISNULL(f.cpf, ''))
                                             when 'J' then trim(ISNULL(f.cnpj, ''))
                                             else ''
                                            end as cpfCnpj,
                                            count(*)
                                        FROM bethadba.fornecedores f
                                        where cpfCnpj <> ''
                                        GROUP by cpfCnpj, tipo
                                        HAVING COUNT(*) > 1
                """)

                if len(busca) > 0:
                    for i in busca:
                        list_i_fornecedor = i['i_fornecedor'].split(',')

                        for indice in range(0, len(list_i_fornecedor)):
                            i_fornecedor = list_i_fornecedor[indice]
                            tipo = i['tipo']

                            match tipo:
                                case "J":
                                    newCnpj = geraCnpj()

                                    dadoAlterado.append(f"Alterado o CNPJ do Fornecedor {i_fornecedor} para {newCnpj}")
                                    comandoUpdate += f"UPDATE bethadba.fornecedores set cnpj = '{newCnpj}' where i_fornecedor = {i_fornecedor};\n"
                                case "F":
                                    newCpf = geraCfp()

                                    dadoAlterado.append(f"Alterado o CPF do Fornecedor {i_fornecedor} para {newCpf}")
                                    comandoUpdate += f"UPDATE bethadba.fornecedores set cpf = '{newCpf}' where i_fornecedor = {i_fornecedor};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_cpf_duplicado: {e}")
            return

        if cnpj_cpf_duplicado:
            dado = analisa_cnpj_cpf_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_cpf_duplicado(listDados=dado)

    def analisa_corrige_inscricao_estadual_duplicada(pre_validacao):
        nomeValidacao = "Fornecedor com Inscrição Estadual duplicada"

        def analisa_inscricao_estadual_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_inscricao_estadual_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                                        SELECT 
                                            f.iestadual,
                                            count(*) as quantidade,
                                            list(f.i_fornecedor) as i_fornecedor
                                        from bethadba.fornecedores f 
                                        where f.iestadual is not null
                                        and f.iestadual <> ''                                
                                        group by f.iestadual
                                        HAVING count(*) > 1
                                        """)

                if len(busca) > 0:
                    for i in busca:
                        list_i_fornecedor = i['i_fornecedor'].split(',')

                        for indice in range(0, len(list_i_fornecedor)):
                            i_fornecedor = list_i_fornecedor[indice]

                            newIE = geraInscricaoEstadual()
                            dadoAlterado.append(f"Alterada a Inscrição Estadual do Fornecedor {i_fornecedor} para {newIE}")
                            comandoUpdate += f"UPDATE bethadba.fornecedores set iestadual = '{newIE}' where i_fornecedor = {i_fornecedor};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_inscricao_estadual_duplicada: {e}")
            return

        if inscricao_estadual_duplicada:
            dado = analisa_inscricao_estadual_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_inscricao_estadual_duplicada(listDados=dado)

    def analisa_corrige_inscricao_municipal_duplicada(pre_validacao):
        nomeValidacao = "Fornecedor com Inscrição Municipal duplicada"

        def analisa_inscricao_municipal_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_inscricao_municipal_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                                           SELECT 
                                                f.imunicipal,
                                                count(*) as quantidade,
                                                list(f.i_fornecedor) as i_fornecedor
                                            from bethadba.fornecedores f 
                                            where f.imunicipal is not null
                                            and f.imunicipal <> ''                     
                                            group by f.imunicipal
                                            HAVING count(*) > 1
                """)

                if len(busca) > 0:
                    for i in busca:
                        list_i_fornecedor = i['i_fornecedor'].split(',')

                        for indice in range(0, len(list_i_fornecedor)):
                            i_fornecedor = list_i_fornecedor[indice]

                            newIM = generateInscricaoMunicipal()
                            dadoAlterado.append(f"Alterada a Inscrição Municipal do Fornecedor {i_fornecedor} para {newIM}")
                            comandoUpdate += f"UPDATE bethadba.fornecedores set imunicipal = '{newIM}' where i_fornecedor = {i_fornecedor};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_inscricao_municipal_duplicada: {e}")
            return

        if inscricao_municipal_duplicada:
            dado = analisa_inscricao_municipal_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_inscricao_municipal_duplicada(listDados=dado)

    def analisa_corrige_data_situacao_maior_data_atual(pre_validacao):
        nomeValidacao = "Fornecedor possui a Data Situação maior que a data atual"

        def analisa_data_situacao_maior_data_atual():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_situacao_maior_data_atual(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_fornecedor,
                        datasituacao,
                        date(NOW()) as data_atual,
                        i_entidades
                    from bethadba.fornecedores
                    where datasituacao > data_atual
                """)

                if len(busca) > 0:
                    df = pd.DataFrame(busca)
                    for row in df.itertuples():
                        dadoAlterado.append(f"Alterada a data da situação do Fornecedor {row.i_fornecedor} de {row.datasituacao} para {row.data_atual}")
                        comandoUpdate += f"UPDATE bethadba.fornecedores set datasituacao = '{row.data_atual}' where i_fornecedor = {row.i_fornecedor} and i_entidades = {row.i_entidades};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_situacao_maior_data_atual: {e}")
            return

        if data_situacao_maior_data_atual:
            dado = analisa_data_situacao_maior_data_atual()

            if corrigirErros and len(dado) > 0:
                corrige_data_situacao_maior_data_atual(listDados=dado)

    if dadosList:
        analisa_corrige_cnpj_cpf_invalido(pre_validacao="cnpj_cpf_invalido")
        analisa_corrige_fornecedor_estado_nulo(pre_validacao="fornecedor_estado_nulo")
        analisa_corrige_cnpj_cpf_duplicado(pre_validacao="cnpj_cpf_duplicado")
        analisa_corrige_inscricao_estadual_duplicada(pre_validacao="inscricao_estadual_duplicada")
        analisa_corrige_inscricao_municipal_duplicada(pre_validacao="inscricao_municipal_duplicada")
        analisa_corrige_data_situacao_maior_data_atual(pre_validacao="data_situacao_maior_data_atual")

    # Executa idependente de existir na tabela de controle
    analisa_corrige_cnpj_cpf_nulo(pre_validacao='cnpj_cpf_nulo')
    return
