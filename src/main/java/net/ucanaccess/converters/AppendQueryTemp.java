package net.ucanaccess.converters;

import static com.healthmarketscience.jackcess.impl.query.QueryFormat.NEWLINE;

import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.jackcess.impl.query.AppendQueryImpl;


public class AppendQueryTemp extends AppendQueryImpl {

	
	public AppendQueryTemp(AppendQueryImpl impl) {
		super(impl.getName(), impl.getRows(), impl.getObjectId(), impl.getObjectFlag());
   }
	
	 protected void toSQLString(StringBuilder builder)
	  {
	    builder.append("INSERT INTO ");
	    toOptionalQuotedExpr(builder, getTargetTable(), true);
	    toRemoteDb(builder, getRemoteDbPath(), getRemoteDbType());
	    builder.append(NEWLINE);
	    List<String> values = getValues();
	    if(!values.isEmpty()) {
	      List<String> decl= getInsertDeclaration();
	      if(decl.size()>0){
	        builder.append("(").append(getInsertDeclaration()).append(")");
	      }
	      builder.append(" VALUES (").append(values).append(')');
	    } else {
	    	 List<String> decl= getSelectDeclaration();
		      if(decl.size()>0){
		        builder.append("(").append(getSelectDeclaration()).append(")");
		      }
	    	toSQLSelectString(builder, true);
	    }
	  }

	private  List<String> getInsertDeclaration() {
	      return new RowFormatter(getDeclaration(getValueRows())) {
	    	   @Override protected void format(StringBuilder builder, Row row) {
		           String column=row.name2;   
		           if(!(column.startsWith("[")&&column.endsWith("]"))){
		        	   column="["+row.name2+"]";
		           }
	    		   builder.append(column);
		        }
		      }.format();
	}
	
	private  List<String> getSelectDeclaration() {
	      return new RowFormatter(getDeclaration(getColumnRows())) {
	    	   @Override protected void format(StringBuilder builder, Row row) {
		           String column=row.name2;   
		           if(!(column.startsWith("[")&&column.endsWith("]"))){
		        	   column="["+row.name2+"]";
		           }
	    		   builder.append(column);
		        }
		      }.format();
	}

	private List<Row> getDeclaration(List<Row> valueRows) {
		ArrayList<Row> ardc=new ArrayList<Row>();
		for(Row row:valueRows){
			if(row.name2!=null){
				ardc.add(row);
			}
		}
		return ardc;
	}
	
	
	

}
