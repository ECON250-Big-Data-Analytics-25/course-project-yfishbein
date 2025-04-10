{% macro wiki_is_meta_page(title) %}
CASE 
    WHEN {{ title }} like 'Категорія:%' THEN TRUE
    WHEN {{ title }} like 'Вікіпедія:%' THEN TRUE
    WHEN {{ title }} like 'Файл:%' THEN TRUE
    WHEN {{ title }} like 'Шаблон:%' THEN TRUE
    WHEN {{ title }} like 'Спеціальна:%' THEN TRUE
    WHEN {{ title }} like 'Портал:%' THEN TRUE
    ELSE FALSE
END
{% endmacro %}