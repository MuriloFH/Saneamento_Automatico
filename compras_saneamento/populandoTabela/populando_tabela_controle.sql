-- orgao_nao_cadastrado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'orgao_nao_cadastrado'
where tipo_registro = 'configuracoes-organogramas'
and mensagem_erro like '%não possui órgão cadastrado%';

-- orgao_nao_cadastrado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'unidade_nao_cadastrada'
where tipo_registro = 'configuracoes-organogramas'
and mensagem_erro like '%não possui unidade cadastrada%';

-- orgao_nao_cadastrado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'centro_custo_nao_cadastrado'
where tipo_registro = 'configuracoes-organogramas'
and mensagem_erro like '%não possui centro de custo cadastrado%';

--materiais_tipo_servicos_nao_estocavel
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'materiais_tipo_servicos_nao_estocavel'
where tipo_registro = 'materiais'
and mensagem_erro like '%do tipo SERVIÇO, não pode ser ESTOCÁVEL%';

-- prazos_entrega_maior_quatro_digitos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'prazos_entrega_maior_quatro_digitos'
where tipo_registro = 'prazos-entrega'
and mensagem_erro like '%Não é permitido mais de 4 dígitos no campo Prazo de entrega%';

-- descricao_prazos_entrega_maior_cinquenta_digitos
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_prazos_entrega_maior_cinquenta_digitos'
where tipo_registro = 'prazos-entrega'
and mensagem_erro like '%Não é permitido mais de 50 caracteres no campo prazo de entrega para%';

-- local_entrega_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'local_entrega_nulo'
where tipo_registro = 'locais-entrega'
and mensagem_erro like '%Possui local de entrega nulo. Deverá ser informado um local de entrega%';

-- solicitacao_compra_possui_data_diferente_do_exercicio
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'solicitacao_compra_possui_data_diferente_do_exercicio'
where tipo_registro = 'solicitacoes'
and mensagem_erro like '%possui data diferente do exercício a qual se refere%';

-- responsavel_cpf_nulo_ou_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'responsavel_cpf_nulo_ou_invalido'
where tipo_registro = 'responsaveis'
and mensagem_erro like '%é inválido%';

-- ato_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ato_nulo'
where tipo_registro = 'atos'
and mensagem_erro like '%O número do ato não foi informado no cadastro%';

-- ato_data_designacao_ou_publicacao_nula
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ato_data_designacao_ou_publicacao_nula'
where tipo_registro = 'atos'
and mensagem_erro like '%A data de designação ou de publicação não foi informada no cadastro%';

-- fornecedor_endereco_incompleto
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_endereco_incompleto'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%não possui o código da cidade%';

-- fornecedor_inscricao_municipal_duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_inscricao_municipal_duplicada'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%possui a INSCRIÇÃO MUNICIPAL%';

-- fornecedor_inscricao_estadual_duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_inscricao_estadual_duplicada'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%possui a INSCRIÇÃO ESTADUAL%';

-- fornecedor_cpf_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_cpf_invalido'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%O CPF%';

-- fornecedor_mais_de_um_email
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_mais_de_um_email'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%Não é permitido mais de um e-mail para o fornecedor%';

-- cpf_autoridade_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_autoridade_invalido'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%O cpf da autoridade da comissão%';

-- cpf_presidente_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_presidente_invalido'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%O cpf do presidente da comissão%';

-- data_expiracao_nula
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_expiracao_nula'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%A data de expiração da comissão não pode ser nula%';

-- tipo_comissao_diferente_do_pregao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tipo_comissao_diferente_do_pregao'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%tipo de comissão diferente de Pregão%';

-- comissao_sem_data_criacao_ou_designacao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'comissao_sem_data_criacao_ou_designacao'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%não possui data de criação ou designação%';

-- comissao_sem_responsavel
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'comissao_sem_responsavel_assinatura'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%não possui um responsavel%';

-- tipo_comissao_diferente_leiloeiro
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tipo_comissao_diferente_leiloeiro'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%possui um tipo de comissão diferente de Leiloeiro%';

-- data_exoneracao_inferior_data_processo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_exoneracao_inferior_data_processo'
where tipo_registro = 'comissoes-licitacao'
and mensagem_erro like '%A exoneração da comissão não deve ser inferior aos processos atrelados%';

-- processo de compra com licitação duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'licitacao_duplicada'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%possui o mesmo NÚMERO DE LICITAÇÃO%';

-- ano de contratação inferior ao ano do processo administrativo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ano_contratacao_menor_ano_processo'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%O ano da contratação não pode ser menor que o ano do processo administrativo%';

-- data/hora de abertura da licitação igual ou inferior a data/hora de inicio do recebimento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_hora_abertura_licitacao_menor_inicio_recebimento'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%A data/hora de abertura da licitação não pode ser igual ou menor que a data/hora de início do recebimento%';

-- data/hora de abertura da licitação igual ou inferior a data/hora de final do recebimento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_hora_abertura_licitacao_menor_final_recebimento'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%A data de abertura dos envelopes não pode ser igual ou menor que a data/hora final de recebimento!%';

-- A data de início de recebimento dos envelopes não pode ser menor que a data do processo!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_inicio_recebimento_menor_data_processo'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%A data de início de recebimento dos envelopes não pode ser menor que a data do processo!%';

-- A data de início de recebimento dos envelopes não pode ser menor que a data do processo!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_expiracao_comissao_menor_data_processo'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%A data de expiração da comissão que foi informada no processo não pode ser menor que a data do processo!%';

-- Processo com ata de registro de preço sem data de homologação!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_ata_sem_data_homologacao'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%com Ata de Registro de Preço gerada NÃO possui data de homologação!%';

-- Processo com data de homologação e sem participantes!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_sem_participante_com_data_homologacao'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%possui data de homologação informada, porém, não possui participantes com propostas!%';

-- Processo homologado porem sem ata de registro de preço mas há adjudicações!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_homologado_adjudicado_sem_ata'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%está homologado, porém não foi gerada a ata de registro de preços mas há adjudicações%';

-- Exige que processos de dispensa de licitacao e inexibilidade com registro de preço tenham inciso relacionado à lei 14.133/2021.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fundamento_processo_com_registro_preco_em_modalidade_especifica'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%não permite registro de preços para esta modalidade.%';

-- não permite registro de preços, para Registros de Preços o tipo do objeto deverá ser somente Compras e Serviços ou Obras e Serviços de Engenharia.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tipo_processo_nao_permite_registro_preco_informado'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%não permite registro de preços, para Registros de Preços o tipo do objeto deverá ser %';

-- Nome Órgão Gerenciador informado com mais de 60 caracteres.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'nome_orgao_processo_maior_60_caracter'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%possui o Nome Órgão Gerenciador informado com mais de 60 caracteres. Botão Dados da Adesão.%';

-- está homologado, porém não possui Ata de Registro de Preços gerada.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_homologado_sem_ata'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%stá homologado, porém não possui Ata de Registro de Preços gerada.%';

-- modalidade Outras e tipo de objeto diferente credenciamento e chamamento.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'modalidade_processo_outras_com_tipo_objeto_diferente_5'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%possui modalidade Outras e tipo de objeto diferente credenciamento e chamamento.%';

-- possui divisão de saldo pendente entre as entidades..
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'saldo_pendente_entre_entidades'
where tipo_registro = 'processos-administrativo-item-entidades'
and mensagem_erro like '%possui divisão de saldo pendente entre as entidades.%';

-- com valor superior a quantidade licitada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'quantidade_item_superior_licitada'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%com valor superior a quantidade licitada%';

-- está sem inciso informado. Para processos de pregão, o inciso é obrigatório.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'incisao_nulo_processos_tipo_pregao'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%está sem inciso informado. Para processos de pregão, o inciso é obrigatório.%';

-- não permitido para a modalidade do processo.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dispensa_pregao_com_inciso_incorreto'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%não permitido para a modalidade do processo%';

-- não está marcado como Registro de Preços, porém possui contratos relacionados com a marcação como Ata de Registro de Preços.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'registro_preco_nulo_com_contrato_marcado_ata_registro_preco'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%não está marcado como Registro de Preços, porém possui contratos relacionados com a marcação como Ata de Registro de Preços.%';

-- possui movimentações no período a ser migrado, mas a coluna data_contrato_transformers da tabela compras.processos não está preenchida. Em caso de não preenchimento, o processo e suas dependências não serão migradas.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%possui movimentações no período a ser migrado, mas a coluna data_contrato_transformers da tabela %';

-- O ano da data da ata deve ser igual ao ano da data do registro de preço.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ano_data_ata_diferente_ano_data_registro_preco'
where tipo_registro = 'atas-registro-preco'
and mensagem_erro like '%O ano da data da ata deve ser igual ao ano da data do registro de preço%';

-- É necessário que o Processo Administrativo XX/XXXX esteja homologado para inserir uma ata de registro de preço.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ata_registro_preco_sem_processo_homologado'
where tipo_registro = 'atas-registro-preco'
and mensagem_erro like '%esteja homologado para inserir uma ata de registro de preço.%';

-- O ano da data da ata deve ser igual ao ano da data do registro de preço.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ano_data_ata_diferente_ano_data_registro_preco'
where tipo_registro = 'atas-registro-preco'
and mensagem_erro like '%O ano da data da ata deve ser igual ao ano da data do registro de preço%';

-- Item do processo configurado com o campo Só MPEs no processo, mais o processo não esta configurado como Favorecimento MPEs
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_so_mpes_sem_configuracao_favorecimento_mpes'
where tipo_registro = 'processos-administrativo-itens'
and mensagem_erro like '%mais o processo não esta configurado como Favorecimento MPEs%';

-- O Item possui cota reservada e o processo não está configurado como Favorecimento MPEs e sim Exclusividade o que não é permitido.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'item_cota_reservada_processo_configuracao_exclusividade'
where tipo_registro = 'processos-administrativo-itens'
and mensagem_erro like '%não está configurado como Favorecimento MPEs e sim Exclusividade o que não é permitido%';

--O lote de processo adminsitrativo, possui lotes diferentes com mesma descrição
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'lote_processo_adm_duplicidade_descricao'
where tipo_registro = 'processos-administrativo-lotes'
and mensagem_erro like '%O lote de processo adminsitrativo, possui lotes diferentes com mesma descrição%';

--A data do protocolo do fornecedor convidado não foi informada (referente a data do convite no cloud)
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_protocolo_convidado_nula'
where tipo_registro = 'convidado-licitacao'
and mensagem_erro like '%A data do protocolo do fornecedor convidado%';

--A data de julgamento da impugnação não foi informada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_julgamento_impugnacao_nula'
where tipo_registro = 'processo-administrativo-impugnacoes'
and mensagem_erro like '%A data de julgamento da impugnação%';

--O Representante do Participante possui CPF invalido.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'representante_participante_cpf_invalido'
where tipo_registro = 'participante-licitacao'
and mensagem_erro like '%possui CPF invalido.%';

--O participante não está convidado para o processo modalidade convite é necessário que o participante esteja convidado para o processo.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'participante_nao_convidado_para_processo'
where tipo_registro = 'participante-licitacao'
and mensagem_erro like '%modalidade convite é necessário que o participante esteja convidado para o processo.%';

--O Credor não possui proposta para todos os itens do processo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'credor_com_itens_sem_proposta'
where tipo_registro = 'proposta-participante'
and mensagem_erro like '%não possui proposta para todos os itens do processo%';

--No processo administrativo a proposta do participante e item possui classificação inválida!";
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'proposta_participante_classificacao_invalida'
where tipo_registro = 'proposta-participante'
and mensagem_erro like '%possui classificação inválida!%';

--Os processo/ano possuem divergência entre a quantidade de participantes e a quantidade de participantes com propostas. Realizar ajuste de maneira que fiquem iguais
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'divergencia_quantidade_participantes_com_e_sem_proposta'
where tipo_registro = 'proposta-participante'
and mensagem_erro like '%possuem divergência entre a quantidade de participantes e a quantidade de participantes com propostas%';

-- possui propostas com situacao 0, 5 ou 7 (Indefinido, Empate ou Classificado) e está homologado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'situacao_proposta_incorreta_para_processos_homologados'
where tipo_registro = 'proposta-participante'
and mensagem_erro like '%possui propostas com situacao 0, 5 ou 7 (Indefinido, Empate ou Classificado) e está homologado%';

--A interposição de recurso do fornecedor do processo precisa estar acatada ou rejeitada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'situacao_interposicao_recurso_invalida'
where tipo_registro = 'processo-administrativo-interposicoes'
and mensagem_erro like '%precisa estar acatada ou rejeitada%';

--A anulação/revogação do processo está com data anterior a data de homologação
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_anulacao_revogacao_processo_anterior_data_homologacao'
where tipo_registro = 'processo-administrativo-atos-finais'
and mensagem_erro like '%está com data anterior a data de homologação%';

--O responsável pela anulação do processo não foi informado!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'responsavel_anulacao_nao_informado'
where tipo_registro = 'processo-administrativo-atos-finais'
and mensagem_erro like '%não possui responsável informad%';


--O processo possui homologação, porém não há nenhum participante vencedor para o processo.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_homologado_sem_participante_vencedor'
where tipo_registro = 'processo-administrativo-atos-finais'
and mensagem_erro like '%possui homologação, porém não há nenhum participante vencedor para o processo%';

--O responsável pela anulação/revogação do processo não foi informado!
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'responsavel_revogacao_nao_informado'
where tipo_registro = 'processo-administrativo-atos-finais'
and mensagem_erro like '%O responsável pela anulação/revogação do processo%';

-- O veiculo publicação do tipo empresa contratada não possui processo de compra informado.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'processo_compra_nulo'
where tipo_registro = 'veiculos-publicacao'
and mensagem_erro like '%não possui processo de compra informado!%';

-- contrato_sem_itens.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'contrato_sem_itens'
where tipo_registro = 'contratacoes'
and mensagem_erro like '%não possui itens%' and mensagem_erro like '%O contrato%';

--O lote de processo adminsitrativo, possui lotes diferentes com mesma descrição
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'lote_processo_adm_duplicidade_descricao'
where tipo_registro = 'processos-administrativo-lotes'
and mensagem_erro like '%O lote de processo adminsitrativo, possui lotes diferentes com mesma descrição%';

--A data do protocolo do fornecedor convidado não foi informada (referente a data do convite no cloud)
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_protocolo_convidado_nula'
where tipo_registro = 'convidado-licitacao'
and mensagem_erro like '%A data do protocolo do fornecedor convidado%';

--A data de julgamento da impugnação não foi informada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_julgamento_impugnacao_nula'
where tipo_registro = 'processo-administrativo-impugnacoes'
and mensagem_erro like '%A data de julgamento da impugnação%';

-- Não está de acordo com distribuição de saldo entre entidades participantes do processo.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'saldo_item_diferente_distribuicao_saldo_entre_entidades'
where tipo_registro = 'contratacoes-itens'
and mensagem_erro like '%não está de acordo com distribuição de saldo entre entidades participantes do processo%';

--  está vínculado a mais de um contrato e está extrapolando o saldo estipulado em processo.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo'
where tipo_registro = 'contratacoes-itens'
and mensagem_erro like '% está vínculado a mais de um contrato e está extrapolando o saldo estipulado em processo.%';

-- A data de vigência do aditivo não foi informada.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'vigencia_aditivo_nulo'
where tipo_registro = 'contratacoes-aditivos'
and mensagem_erro like '%A data de vigência do aditivo não foi informada.%';

--Liquidação da AF possuí documento fiscal com valor zero.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'liquidacao_af_documento_zerado'
where tipo_registro = 'comprovantes'
and mensagem_erro like '%possuí documento fiscal com valor zero%';

--O contrato foi rescindido porem já havia vencido.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'rescisao_contrato_vencido'
where tipo_registro = 'contratacoes-rescisoes'
and mensagem_erro like '%porem o contrato já havia vencido%';

-- diferente da data do contrato principal. A natureza não é de prazo.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'vigencia_final_aditivo_diferente_dt_contrato_principal'
where tipo_registro = 'contratacoes-aditivos'
and mensagem_erro like '%diferente da data do contrato principal. A natureza não é de prazo.%';

-- não possui itens.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'aditivo_sem_item'
where tipo_registro = 'aditivos-itens'
and mensagem_erro like '%não possui itens%';

-- aditivo_de_prazo_com_itens
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'aditivo_de_prazo_com_itens'
where tipo_registro = 'aditivos-itens'
and mensagem_erro like '%possui itens informados, porem sua natureza prevê apenas aditivo de Prazo.%';

-- data_historico_fora_vigencia_contrato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_historico_fora_vigencia_contrato'
where tipo_registro = 'contratacoes-apostilamentos'
and mensagem_erro like '%possui a data de histórico fora da vigência do contrato.%';

-- data_sf_menor_data_inicio_contrato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_sf_menor_data_inicio_contrato'
where tipo_registro = 'solicitacoes-fornecimento'
and mensagem_erro like '%possui data anterior ao início de vigência do contrato%';

-- data_sf_menor_data_fim_contrato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_sf_menor_data_fim_contrato'
where tipo_registro = 'solicitacoes-fornecimento'
and mensagem_erro like '%possui data posterior ao fim de vigência do contrato%';

-- data_homologacao_processo_maior_data_sf
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_homologacao_processo_maior_data_sf'
where tipo_registro = 'solicitacoes-fornecimento'
and mensagem_erro like '%Inconsistência: A data de homologação não pode ser maior que a data da AF, processo%';

-- sf_sem_contrato_informado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'sf_sem_contrato_informado'
where tipo_registro = 'solicitacoes-fornecimento'
and mensagem_erro like '% não possui contrato informado, mas é originada de um processo que possui mais de um contrato para o fornecedor%';

-- item_nao_presente_no_contrato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'item_nao_presente_no_contrato'
where tipo_registro = 'solicitacao-fornecimento-itens'
and mensagem_erro like '%Não foi possível localizar o item %';

-- qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato'
where tipo_registro = 'solicitacoes-fornecimento-itens'
and mensagem_erro like '%O total das solicitações de fornecimento do item %';

--Liquidação da AF possuí documento fiscal com valor zero.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'liquidacao_af_documento_zerado'
where tipo_registro = 'comprovantes'
and mensagem_erro like '%possuí documento fiscal com valor zero%';

--O contrato foi rescindido porem já havia vencido.
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'rescisao_contrato_vencido'
where tipo_registro = 'contratacoes-rescisoes'
and mensagem_erro like '%porem o contrato já havia vencido%';

-- qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato'
where tipo_registro = 'solicitacao-fornecimento-itens'
and mensagem_erro like '%O total das solicitações de fornecimento do item %';

-- data_inicio_recebimento_menor_data_final_recebimento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_inicio_recebimento_menor_data_final_recebimento'
where tipo_registro = 'processos-administrativo'
and mensagem_erro like '%A data de início de recebimento dos envelopes não pode ser maior que a data final de recebimento dos envelopes!%';

-- fornecedor_ata_diferente_fornecedor_vencedor_processo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_ata_diferente_fornecedor_vencedor_processo'
where tipo_registro = 'atas-registro-preco'
and mensagem_erro like '%não é o mesmo fornecedor, vencedor, que esta sendo informado na ATA, que é o fornecedor%';

-- credor_af_diferente_da_compra_direta
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'credor_af_diferente_da_compra_direta'
where tipo_registro = 'solicitacoes-fornecimento'
and mensagem_erro like '%possui fornecedor diferente da Compra Direta.%';

commit;