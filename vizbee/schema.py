schedule = dict(
    type='string',
    regex=r'(\d+) (days|hours|minutes|seconds)',
)


schema = dict(
    connections=dict(
        type='dict',
        required=True,
        nullable=False,
        keyschema=dict(
            type='string',
            regex='[a-z\_]+',
        ),
    ),

    datasets=dict(
        type='dict',
        required=True,
        nullable=False,
        keyschema=dict(
            type='string',
            regex='[a-z\-]+',
        ),

        valueschema=dict(
            type='dict',
            schema=dict(
                name=dict(type='string'),

                query=dict(
                    type='string',
                    required=True,
                    nullable=False,
                ),

                graph=dict(
                    type='dict',
                    required=False,
                    nullable=True,
                ),

                schedule=schedule,
            ),
        ),
    ),

    dashboards=dict(
        type='dict',
        required=False,
        nullable=False,
        keyschema=dict(
            type='string',
            regex='[a-z\-]+',
        ),

        valueschema=dict(
            type='dict',
            schema=dict(
                name=dict(type='string'),

                datasets=dict(
                    type='list',
                    required=True,
                    nullable=False,
                ),
            ),
        ),
    ),

    schedule=schedule,
)
