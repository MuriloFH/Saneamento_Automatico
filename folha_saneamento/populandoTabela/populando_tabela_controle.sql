--A rua consta com descricao repetidas
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'duplicidade_logradouro'
where tipo_registro = '01-logradouro'
and mensagem_erro like '%consta com descricao repetidas%';

--Busca as pessoas com data de nascimento maior que data de admissão
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_nascimento_maior_data_admissao'
where tipo_registro = '02-Pessoas'
and mensagem_erro like '%Data de Nascimento%';

--Busca a data de vencimento da CNH menor que a data de emissão da 1ª habilitação!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_vencimento_cnh_menor_data_emissao_1_habilitacao'
where tipo_registro = '03-Pessoas'
and mensagem_erro like '%Data de Vencimento CNH%';

--Busca os campos adicionais com descrição repetido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'duplicidade_descricao_caracteristica'
where tipo_registro = '04-Campos-Adicionais'
and mensagem_erro like '%Codigo da Caracteristica%';

--Pessoas com data de nascimento maior que data de dependencia
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_nascimento_maior_data_dependencia'
where tipo_registro = '06-Pessoas'
and mensagem_erro like '%Data de inicio dependencia%';

--Verifica dependente grau de parentesco(1-Filho(a)/6-Neto/8-Menor Tutelado/11-Bisneto) com data de nascimento MENOR que a do seu responsável.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_data_nascimento_menor_data_nascimento_responsavel'
where tipo_registro = '07-Dependente'
and mensagem_erro like '%tipo Grau Parentesco%';

--cpf_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_duplicado'
where tipo_registro = '08-Pessoas-Fisicas'
and mensagem_erro like '%Quantidade%';

--pis duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pis_duplicado'
where tipo_registro = '09-Pessoas-Fisicas'
and mensagem_erro like '%PIS%';

--pis invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pis_invalido'
where tipo_registro = '10-Pessoas-Fisicas'
and mensagem_erro like '%PIS%';

--cnpj nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_nulo'
where tipo_registro = '11-Pessoas-Juridicas'
and mensagem_erro like '%pessoa%';

--Verifica a descrição dos logradouros que tem caracter especial no inicio da descrição
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'logradouro_caracter_especial_inicio_descricao'
where tipo_registro = '12-Logradouros'
and mensagem_erro like '%Logradouros%';

--Verifica os bairros com descrição repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_duplicada'
where tipo_registro = '13-Bairros'
and mensagem_erro like '%Logradouros%';

--Verifica os nomes dos tipos bases repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_duplicada'
where tipo_registro = '14-Tipos-Bases'
and mensagem_erro like '%Codigo do tipo de bases%';

--Verifica os logradouros sem cidades
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'logradouro_sem_cidade'
where tipo_registro = '15-Logradouros'
and mensagem_erro like '%Codigo do Logradouro%';

--Verifica os atos com número nulos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_nulo'
where tipo_registro = '16-Atos'
and mensagem_erro like '%Codigo do Ato%';

--Verifica os atos repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'duplicidade_numero_tipo_ato'
where tipo_registro = '17-Atos'
and mensagem_erro like '%Codigo do Ato%';

--Verifica os CBO's nulos nos cargos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cbo_nulo'
where tipo_registro = '18-Cargos';

--Verifica categoria eSocial nulo no vínculo empregatício
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'categoria_esocial_nulo'
where tipo_registro = '19-Vínculo-empregaticio';

--Renomeia os vinculos empregaticios repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'duplicidade_vinculo_empregaticio'
where tipo_registro = '20-Vinculos-empregaticios';

--Verifica categoria eSocial nulo no motivo de rescisão
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'motivo_rescisao_sem_categoria_esocial'
where tipo_registro = '21-Motivo-rescisao';

--Verifica as folha que não foram fechadas conforme competência passada por parâmetro
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'folha_sem_fechamento'
where tipo_registro = '22-Folha';

--Verifica as folhas de ferias sem data de pagamento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'folha_ferias_sem_data_pagamento'
where tipo_registro = '23-Ferias';

--Verifica categoria eSocial nulo no motivo de aposentadoria
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'motivo_aposentadoria_sem_categoria_esocial'
where tipo_registro = '24-Motivo-aposentadoria';

--Verifica históricos salariais com salário zerado ou nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'historico_salarial_com_salario_zerado'
where tipo_registro = '25-Historicos-salariais';

--Verificar variáveis com data inicial ou final maior que a data de rescisão do funcionário
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'variaveis_dt_inicial_ou_final_maior_dt_rescisao'
where tipo_registro = '26-Variaveis';

--Busca as movimentações de pessoal repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'movimentacao_pessoal_duplicada'
where tipo_registro = '27-Movimentações-pessoal';

--Busca os tipos de afastamentos repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'afastamentos_duplicados'
where tipo_registro = '28-Afastamentos';

--Busca as alterações de históricos dos funcionários maior que a data de rescisão"
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'alteracao_historico_funcionario_maior_data_rescisao'
where tipo_registro = '29-Historicos-funcionarios';

-- Busca as alterações de salário dos funcionários maior que a data de rescisão
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'alteracao_salario_funcionario_maior_data_rescisao'
where tipo_registro = '30-Funcionarios';

--Busca as alterações de cargo dos funcionários maior que a data de rescisão"
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'alteracao_cargo_funcionario_maior_data_rescisao'
where tipo_registro = '31-Funcionarios';

--Busca as classificações que estão com código errado no tipo de afastamento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tipo_afastamento_com_classificacao_invalida'
where tipo_registro = '32-Afastamento';

--Busca os tipos de atos repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'duplicidade_descricao_tipo_ato'
where tipo_registro = '33-Atos';

--Buscar níveis de organogramas com separadores nulos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'niveis_organograma_separador_invalido'
where tipo_registro = '34-Niveis-organogramas';

--Verifica a natureza de texto jurídico se é nulo nos atos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'natureza_texto_juridico_nula'
where tipo_registro = '35-Atos';

--Verifica se a data de fonte de divulgação é menor que a data de publicação do ato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_fonte_divulgacao_menor_data_publicacao_ato'
where tipo_registro = '36-Atos';

--Ter ao menos um tipo de afastamento na configuração do cancelamento de férias
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cancelamento_ferias_sem_tipo_afastamento'
where tipo_registro = '37-Ferias';

--Verifica descrição de configuração de organograma se é maior que 30 caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'config_organograma_descricao_maior_30_caracteres'
where tipo_registro = '38-Config-Organograma';

--Verifica descrição de configuração de organograma repetido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'config_organograma_descricao_duplicada'
where tipo_registro = '39-Config-Organograma';

--Verifica os RG's repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'rg_duplicado'
where tipo_registro = '40-Pessoas-Fisicas';

--Verifica os cargos com descrição repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cargo_descricao_duplicada'
where tipo_registro = '41-Cargos';

--Verifica o término de vigência maior que 2099 na tabela   bethadba.bases_calc_outras_empresas
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'bases_calc_outras_empresas_vigencia_invalida'
where tipo_registro = '42-Base-Calculo';

--Verifica o número de endereço se está vazio
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'endereco_sem_numero'
where tipo_registro = '43-Endereço';

--Verifica o nome de rua se está vazio
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'rua_sem_descricao'
where tipo_registro = '44-Rua';

--Verifica os funcionários sem previdência
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_sem_previdencia'
where tipo_registro = '45-Funcionarios';

--Verifica os eventos de média vantagem que não tem eventos vinculados
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'evento_media_vantagem_sem_composicao'
where tipo_registro = '46-Media-vantagem';

--Verifica os eventos de média/vantagem se estão compondo outros eventos de média/vantagem
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'evento_media_vantagem_composicao_invalida'
where tipo_registro = '47-Media-vantagem';

--Verifica a data de admissão da matrícula se é posterior a data de início da matrícula nesta lotação física
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica'
where tipo_registro = '48-Funcionarios';

--Verifica o motivo nos afastamentos se contém no máximo 150 caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'afastamento_motivo_maximo_150_caracteres'
where tipo_registro = '49-Afastamentos';

--Verifica a data inicial no afastamento se é maior que a data final
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_inicial_afastamento_maior_data_final'
where tipo_registro = '50-Ferias';

--Verifica as rescisões de aposentadoria com motivo nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'rescisoes_aposentadoria_motivo_nulo'
where tipo_registro = '51-Rescisoes';

--Verifica os grupos funcionais repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'grupos_funcionais_duplicados'
where tipo_registro = '52-Grupos-funcionais';

--Verifica se o número de telefone na lotação física é maior que 9 caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_telefone_maior_9_caracteres'
where tipo_registro = '54-Lotacao-fisica';

--Busca os atos com data inicial nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ato_data_inicial_nulo'
where tipo_registro = '55-Atos';

--Busca as descrições repetidas dos níveis salariais
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'niveis_salariais_com_descricao_duplicada'
where tipo_registro = '56-Niveis-salariais';

--Busca os funcionários com data de nomeação maior que a data de posse
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_data_nomeacao_maior_data_posse'
where tipo_registro = '57-Funcionarios';

--Busca as contas bancárias dos funcionários que estão inválidas
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_conta_bancaria_invalida'
where tipo_registro = '58-Historico-funcionarios';

--Busca os históricos de funcionários com mais do que uma previdência informada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_com_mais_de_uma_previdencia'
where tipo_registro = '59-Historicos-funcionarios';

--Busca os afastamentos com data inicial menor que data de admissão
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'afastamentos_dt_afastamento_menor_dt_admissao'
where tipo_registro = '60-Afastamentos';

--Busca os dependentes sem motivo de término
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_sem_motivo_de_termino'
where tipo_registro = '61-Dependentes';

--Busca os cargos sem configuração de férias
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cargo_sem_configuracao_de_ferias'
where tipo_registro = '62-Cargos';

--A quantidade de vagas não pode ser maior que 9999
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'quantidade_vaga_maior_9999'
where tipo_registro = '63-Cargos';

--Campo observação maior 150 caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'observacao_maior_150_caracteres'
where tipo_registro = '64-Caracteristicas';

--Tipo Administrativo com teto remuneratório nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tipo_adm_com_teto_remuneratorio_nulo'
where tipo_registro = '65-Historico-tipo-adm';

--Verifica horas mês zerado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'historico_com_horas_mes_zerada'
where tipo_registro = '66-Historicos_salariais';

--VERIFICA DATA DE FERIAS COM DATA DE ADMISSÃO
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ferias_dt_gozo_ini_menor_dt_admissao'
where tipo_registro = '67-Ferias';

--Quando a forma de pagamento for Crédito em conta é necessário informar a conta bancária
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'forma_pagamento_credito_sem_conta_vinculada'
where tipo_registro = '68-Historico-funcionarios';

--Verifica a descrição de motivo de alteração salarial repetindo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_duplicada'
where tipo_registro = '69-Motivo-alteração-salarial';

--retorna os funcionários que possuem férias com data de fim do gozo igual ou após a data da rescisão
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ferias_dt_gozo_fin_maior_rescisao'
where tipo_registro = '70-Funcionarios';

--Verifica data fim na lotação física deve ser maior que a data início
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_fim_menor_data_inicio'
where tipo_registro = '72-Local-de-Trabalho';

--Verifica dependente grau de parentesco(3-Pai/Mãe/4-Avô/Avó/12-Bisavô/Bisavó) com data de nascimento MAIOR que a do seu responsável
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_data_nascimento_maior_data_nascimento_responsavel'
where tipo_registro = '73-Dependente';

--Estagiário(s) sem número da apólice de seguro informado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'estagiario_sem_numero_apolice_informado'
where tipo_registro = '75-Estagiarios';

--Verifica Estagiário(s) sem agente de integração informado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'estagiario_agente_integracao_nulo'
where tipo_registro = '76-Estagiarios';

--Verifica Estagiário(s) sem responsável informado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'estagiario_sem_responsavel'
where tipo_registro = '77-Estagiarios';

--Tipo do afastamento deve ter uma movimentação de pessoal informada quando o afastamento possuir um ato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tipos_afast_sem_tipo_movto_pessoal'
where tipo_registro = '78-Afastamentos';

--Verifica em férias no dia da rescisão
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ferias_gozo_final_apos_rescisao'
where tipo_registro = '79-Funcionarios';

--Verifica o motivo do cancelamento de férias não pode ter mais de 50 caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'motivo_cancelamento_maior_50_caracteres'
where tipo_registro = '80-Ferias';

--Verifica  a inscrição municipal da pessoa jurídica que possui mais de 9 digitos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'inscricao_municipal_superior_9_caracteres'
where tipo_registro = '81-Pessoa-juridica';

--Verifica CNPJ inválido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_invalido'
where tipo_registro = '82-Pessoa-juridica';

--Veririca pessoas jurídicas com cnpjs duplicados
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_duplicado'
where tipo_registro = '83-Pessoa-juridica';

--Veririca pessoas fisicas com data de nascimento e nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_nascimento_nulo'
where tipo_registro = '84-Pessoa-Fisica';

--Veririca pessoas fisicas com nome da afiliação repetidos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'filiacao_duplicada'
where tipo_registro = '85-Pessoa-Fisica';

--Veririca pessoas fisicas com email incorreto
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'email_invalido'
where tipo_registro = '86-Pessoa-Fisica';

--Verificar se o dependente é a mesma pessoa que o responsável
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_mesma_pessoa_responsavel'
where tipo_registro = '87-Pessoa-Fisica';

--Verificar dependentes com data de inicio de dependencia menor que a data de nascimento do dependente
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_data_inicio_menor_data_nascimento'
where tipo_registro = '88-Dependente';

--Verificar dependentes com data de casamento menor que a data de nascimento do dependente
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_dt_casamento_menor_dt_nascimento'
where tipo_registro = '89-Dependente';

--dt_homologacao_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_homologacao_nulo'
where tipo_registro = '120-concurso-processo-seletivo';

--numero_sala_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_sala_nulo'
where tipo_registro = '121-local-avaliacao';

--controle_ponto_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'controle_ponto_nulo'
where tipo_registro = '122-configuracao-rais';

--inscricao_nulo_ou_CEI
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'inscricao_nulo_ou_CEI'
where tipo_registro = '123-configuracao-rais';

--inscricao_nulo_ou_CEI
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'mes_base_nulo'
where tipo_registro = '124-configuracao-rais';

--responsavel_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'responsavel_nulo'
where tipo_registro = '125-configuracao-rais';

--contato_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'contato_nulo'
where tipo_registro = '126-configuracao-rais';

--cnpj_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_nulo'
where tipo_registro = '127-configuracao-rais-campo';

--ordenacao_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ordenacao_nulo'
where tipo_registro = '130-agrupador-evento';

--dt_alteracao_menor_dt_nascimento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_alteracao_menor_dt_nascimento'
where tipo_registro = '131-historico-pessoa-fisica';

--indicativo_educacional_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'indicativo_educacional_nulo'
where tipo_registro = '132-entidades';

--dt_fechamento_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_fechamento_nulo'
where tipo_registro = '133-dataFechamentoFolha';

--beneficiario_nao_cadastrano_na_tabela
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'beneficiario_nao_cadastrano_na_tabela'
where tipo_registro = '137-beneficiario';

--dt_fim_divergente
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_fim_divergente'
where tipo_registro = '138-base-outras-empresas';

--periodo_aquisitivo_ferias_concomitante
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'periodo_aquisitivo_ferias_concomitante'
where tipo_registro = '139-periodo-aquisitivo-ferias';

--dt_homologacao_invalida
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_homologacao_invalida'
where tipo_registro = '141-processo-trabalhista';

--codigo_esocial_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'codigo_esocial_duplicado'
where tipo_registro = '90-Historico-funcionarios';

--multiplos_locais_trabalho_principal
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'multiplos_locais_trabalho_principal'
where tipo_registro = '91-Funcionarios';

--funcionario_sem_local_trabalho_principal
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_sem_local_trabalho_principal'
where tipo_registro = '92-Funcionarios';

--dt_hist_posterior_dt_cessacao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_hist_posterior_dt_cessacao'
where tipo_registro = '93-APOSENTADO';

--hist_clt_sem_opcao_federal
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'hist_clt_sem_opcao_federal'
where tipo_registro = '94-Historico-Matricula';

--dt_inicio_emprestimo_menor_dt_admissao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_inicio_emprestimo_menor_dt_admissao'
where tipo_registro = '95-emprestimo';

--pensionista_menor_18_ano_sem_responsavel
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pensionista_menor_18_ano_sem_responsavel'
where tipo_registro = '96-pensionista';

--num_certidao_maior_32_caracter
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'num_certidao_maior_32_caracter'
where tipo_registro = '97-Pessoas';

--numero_telefone_maior_9_caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_telefone_maior_9_caracteres'
where tipo_registro = '98-Lotacao-fisica';

--config_organograma_sem_nivel
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'config_organograma_sem_nivel'
where tipo_registro = '99-Configuracao-Organograma';

--estagiario_nao_presente_na_tabela_estagios
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'estagiario_nao_presente_na_tabela_estagios'
where tipo_registro = '100-Estagiarios';

--banco_fora_padrao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'banco_fora_padrao'
where tipo_registro = '101-banco';

--logradouro_sem_rua
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'logradouro_sem_rua'
where tipo_registro = '102-Logradouros';

--pensionista_dt_admissao_menor_dt_rescisao_instituidor
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pensionista_dt_admissao_menor_dt_rescisao_instituidor'
where tipo_registro = '103-datadecontrato';

--evento_sem_historico
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'evento_sem_historico'
where tipo_registro = '104-Eventos';

--falta_tipo_1_com_competencia
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'evento_sem_historico'
where tipo_registro = '105-Afastamentos';

--dt_fim_lotacao_maior_dt_fim_contrato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_fim_lotacao_maior_dt_fim_contrato'
where tipo_registro = '107-Funcionario';

--tipo_pensao_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tipo_pensao_nulo'
where tipo_registro = '108-pensionista';

--dt_nascimento_maior_dt_chegada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_nascimento_maior_dt_chegada'
where tipo_registro = '109-Pessoas';

--dt_afastamento_concomitante
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_afastamento_concomitante'
where tipo_registro = '110-Afastamentos';

--ferias_concomitantes
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ferias_concomitantes'
where tipo_registro = '111-Ferias';

--ferias_concomitantes
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_afastamento_concomitante_dt_ferias'
where tipo_registro = '112-Afastamentos';

--falta_concomitante_dt_afastamento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'falta_concomitante_dt_afastamento'
where tipo_registro = '113-Faltas';

--falta_concomitante_dt_ferias
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'falta_concomitante_dt_ferias'
where tipo_registro = '114-Faltas';

--funcionario_sem_historico
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_sem_historico'
where tipo_registro = '115-Funcionario';

--funcionario_sem_historico_cargo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_sem_historico_cargo'
where tipo_registro = '116-Funcionario';

--funcionario_sem_historico_salarial
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'funcionario_sem_historico_salarial'
where tipo_registro = '117-Funcionario';

--faixa_maior_99
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'faixa_maior_99'
where tipo_registro = '118-Licenca-premio';

--lancamento_faltante
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'lancamento_faltante'
where tipo_registro = '119-lancamento-evento';

--lancamento_faltante
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pf_sem_historico'
where tipo_registro = '128-Pessoas';

--dt_vigorar_ato_maior_dt_movimentacao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_vigorar_ato_maior_dt_movimentacao'
where tipo_registro = '129-movimentacao-pessoal';

--lancamento_posterior_rescisao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'lancamento_posterior_rescisao'
where tipo_registro = '134-lancamento-evento';

--lancamento_posterior_cessacao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'lancamento_posterior_cessacao'
where tipo_registro = '135-lancamento-evento';

--aposentado_sem_rescisao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'aposentado_sem_rescisao'
where tipo_registro = '136-aposentado';

--pensionista_nao_cadastrado_na_tabela
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pensionista_nao_cadastrado_na_tabela'
where tipo_registro = '149-pensionista';

--pensionista_sem_cessacao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pensionista_sem_cessacao'
where tipo_registro = '150-pensionista';

--pensionista_sem_dependente
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'pensionista_sem_cessacao'
where tipo_registro = '151-pensionista';

--instituidor_sem_rescisao_por_aposentadoria
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'instituidor_sem_rescisao_por_aposentadoria'
where tipo_registro = '152-pensionista';

--dependente_sem_dt_inicial
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_sem_dt_inicial'
where tipo_registro = '154-Dependentes';

--dependente_motivo_inicial_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_motivo_inicial_nulo'
where tipo_registro = '155-Dependentes';

--dependente_com_mais_de_uma_config_IRRF
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dependente_com_mais_de_uma_config_IRRF'
where tipo_registro = '156-Dependentes';

commit;