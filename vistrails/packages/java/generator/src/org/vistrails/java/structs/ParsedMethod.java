package org.vistrails.java.structs;

import java.util.Collections;
import java.util.List;

public class ParsedMethod {

    public final String name;
    public final int modifiers;
    public final String return_type;
    public final List<ParsedParam> parameters;

    public ParsedMethod(String name, int modifiers, String return_type,
            List<ParsedParam> parameters)
    {
        this.name = name;
        this.modifiers = modifiers;
        this.return_type = return_type;
        this.parameters = Collections.unmodifiableList(parameters);
    }

}
