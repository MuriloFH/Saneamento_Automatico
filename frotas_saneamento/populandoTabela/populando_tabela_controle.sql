-- cpf_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_duplicado'
where tipo_registro = 'funcionarios'
and CHARINDEX('CPF', mensagem_erro) >  0
and CHARINDEX('duplicado com o funcionário', mensagem_erro) >  0;

-- rg_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'rg_duplicado'
where tipo_registro = 'funcionarios'
and CHARINDEX('Identidade', mensagem_erro) >  0
and CHARINDEX('duplicado com o funcionário', mensagem_erro) >  0;

-- cpf_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_invalido'
where tipo_registro = 'funcionarios'
and CHARINDEX('CPF', mensagem_erro) >  0
and CHARINDEX('é inválido', mensagem_erro) >  0;

-- dt_admissao_menor_dt_nascimento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_admissao_menor_dt_nascimento'
where tipo_registro = 'funcionarios'
and CHARINDEX('possui o campo "Data da Admissão" menor ou igual a "Data de Nascimento"!', mensagem_erro) >  0;

-- dt_nascimento_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_nascimento_nulo'
where tipo_registro = 'funcionarios'
and CHARINDEX('não possui o campo "Data de Nascimento" informado!', mensagem_erro) >  0;

-- cpf_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_nulo'
where tipo_registro = 'funcionarios'
and CHARINDEX('não possui CPF informado!', mensagem_erro) >  0;

-- marca_desk
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'marca_desk'
where tipo_registro = 'modelo-veiculo'
and CHARINDEX('não possui o código da marca informado!', mensagem_erro) >  0;

-- cnpj_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_duplicado'
where tipo_registro = 'fornecedores'
and CHARINDEX('duplicado com o fornecedor', mensagem_erro) >  0
and CHARINDEX('CNPJ:', mensagem_erro) >  0;

-- cpf_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_duplicado'
where tipo_registro = 'fornecedores'
and CHARINDEX('duplicado com o fornecedor', mensagem_erro) >  0
and CHARINDEX('CPF:', mensagem_erro) >  0;

-- inscricao_estadual_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'inscricao_estadual_duplicado'
where tipo_registro = 'fornecedores'
and CHARINDEX('possui a INSCRIÇÃO ESTADUAL', mensagem_erro) >  0;

-- cpf_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_invalido'
where tipo_registro = 'fornecedores'
and CHARINDEX('O CPF', mensagem_erro) >  0
and CHARINDEX('é inválido!', mensagem_erro) >  0;

-- cpf_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_nulo'
where tipo_registro = 'fornecedores'
and CHARINDEX('não possui CNPJ/CPF informado.', mensagem_erro) >  0;

-- cnpj_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_nulo'
where tipo_registro = 'fornecedores'
and CHARINDEX('não possui CNPJ/CPF informado.', mensagem_erro) >  0;

-- cpf_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_nulo'
where tipo_registro = 'motoristas'
and CHARINDEX('não possui informação no campo "CPF"!', mensagem_erro) >  0;

-- primeira_cnh_menor_dt_nascimento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'primeira_cnh_menor_dt_nascimento'
where tipo_registro = 'motoristas'
and CHARINDEX('possui o campo "Primeira Habilitação" menor que a "Data de Nascimento"!', mensagem_erro) >  0;

-- dt_emissao_cnh_menor_primeira_cnh
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_emissao_cnh_menor_primeira_cnh'
where tipo_registro = 'motoristas'
and CHARINDEX('possui o campo "Emissão" menor que a "Primeira Habilitação"!', mensagem_erro) >  0;

-- dt_emissao_cnh_menor_dt_nascimento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_emissao_cnh_menor_dt_nascimento'
where tipo_registro = 'motoristas'
and CHARINDEX('possui o campo "Emissão" menor que a "Data de Nascimento"!', mensagem_erro) >  0;

-- cnh_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnh_duplicado'
where tipo_registro = 'motoristas'
and CHARINDEX('duplicado com o funcionário', mensagem_erro) >  0
and CHARINDEX('CNH:', mensagem_erro) >  0;

-- renavan_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'renavan_duplicado'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('possui o RENAVAM:', mensagem_erro) >  0
and CHARINDEX('duplicado com o veículo', mensagem_erro) >  0;

-- chassi_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'chassi_duplicado'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('possui o chassi:', mensagem_erro) >  0
and CHARINDEX('duplicado com o veículo', mensagem_erro) >  0;

-- patrimonio_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'patrimonio_duplicado'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('possui o Número de Patrimônio:', mensagem_erro) >  0
and CHARINDEX('duplicado com o veículo', mensagem_erro) >  0;

-- chassi_maior_20_caracter
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'chassi_maior_20_caracter'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('possui o chassi com o tamanho maior que 20.', mensagem_erro) >  0;

-- consumo_maximo_maior_99
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'consumo_maximo_maior_99'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('consumo máximo maior que o permitido pelo cloud (99.99).', mensagem_erro) >  0;

-- consumo_minimo_maior_99
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'consumo_minimo_maior_99'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('consumo mínimo maior que o permitido pelo cloud (99.99).', mensagem_erro) >  0;

-- dt_aquisicao_menor_dt_fabricacao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_aquisicao_menor_dt_fabricacao'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('contém o ano da data de aquisição', mensagem_erro) >  0;

-- renavam_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'renavam_invalido'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('contendo letras ou caracteres especiais.', mensagem_erro) >  0;

-- veiculo_proprio_fornecedor
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'veiculo_proprio_fornecedor'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('está com vínculo PRÓPRIO e possui proprietário informado.', mensagem_erro) >  0;

-- ano_fabricacao_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ano_fabricacao_invalido'
where tipo_registro = 'veiculo-equipamento'
and CHARINDEX('possui ano de fabricação inválido, o ano de fabricação deve compreender entre 1900 e 2999.', mensagem_erro) >  0;

-- veiculo_sem_combustivel_padrao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'veiculo_sem_combustivel_padrao'
where tipo_registro = 'veiculo-equipamento-combustivel'
and CHARINDEX('não possui combustivel padrão informado.', mensagem_erro) >  0;

-- veiculo_sem_capacidade_volumetrica
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'veiculo_sem_capacidade_volumetrica'
where tipo_registro = 'veiculo-equipamento-combustivel'
and CHARINDEX('não possui capacidade volumétrica de combustível informada.', mensagem_erro) >  0;

-- ano_inicio_veiculo_diferente_ano_centro_custo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ano_inicio_veiculo_diferente_ano_centro_custo'
where tipo_registro = 'veiculo-equipamento-organograma'
and CHARINDEX('é diferente do ano do centro de custo.', mensagem_erro) >  0;

-- dt_inicio_centro_menor_dt_fabricacao_veiculo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_inicio_centro_menor_dt_fabricacao_veiculo'
where tipo_registro = 'veiculo-equipamento-organograma'
and CHARINDEX('é menor que ano de fabricação.', mensagem_erro) >  0;

-- ocorrencia_motorista_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'ocorrencia_motorista_nulo'
where tipo_registro = 'lancamento-ocorrencia'
and CHARINDEX('não possui motorista informado.', mensagem_erro) >  0;

-- despesa_motorista_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'despesa_motorista_nulo'
where tipo_registro = 'lancamento-despesa'
and CHARINDEX('O campo "Motorista" do lançamento da despesa', mensagem_erro) >  0;

-- caractere_especial_documento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'caractere_especial_documento'
where tipo_registro = 'lancamento-despesa'
and CHARINDEX('possui caracteres que não são números!', mensagem_erro) >  0;

-- numero_documento_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_documento_nulo'
where tipo_registro = 'lancamento-despesa'
and CHARINDEX('não foi preenchido.', mensagem_erro) >  0;

-- kilometragem_despesa_inferior_despesa_anterior
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'kilometragem_despesa_inferior_despesa_anterior'
where tipo_registro = 'lancamento-despesa'
and CHARINDEX('possui a kilometragem inferior à(s) despesa(s) anterior(es):', mensagem_erro) >  0;

-- mudanca_odometro_sem_lancamento
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'mudanca_odometro_sem_lancamento'
where tipo_registro = 'lancamento-despesa'
and CHARINDEX('porem a despesa não possui nenhuma ocorrência vinculada.', mensagem_erro) >  0;

-- combustivel_ordem_diferente_combustivel_veiculo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'combustivel_ordem_diferente_combustivel_veiculo'
where tipo_registro = 'ordem-abastecimento-item'
and CHARINDEX('possui combustível diferente do combustível padrão do veículo', mensagem_erro) >  0;

-- valor_unitario_item_despesa_zero
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'valor_unitario_item_despesa_zero'
where tipo_registro = 'lancamento-despesa-item'
and CHARINDEX('não pode ser zero!', mensagem_erro) >  0;

-- codigo_item_despesa_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'codigo_item_despesa_nulo'
where tipo_registro = 'lancamento-despesa-item'
and CHARINDEX('não possui código do material informado.', mensagem_erro) >  0;

-- codigo_material_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'codigo_material_duplicado'
where tipo_registro = 'lancamento-despesa-item'
and CHARINDEX('duplicado com outro item.', mensagem_erro) >  0;

commit;



