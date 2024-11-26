package oracle

import (
	"database/sql"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
}

func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw(
		fmt.Sprintf(`SELECT ORA_DATABASE_NAME as "Current Database" FROM %s`, m.Dialector.(Dialector).DummyTableName()),
	).Row().Scan(&name)
	return
}

func (m Migrator) CreateTable(values ...interface{}) error {
	m.TryQuotifyReservedWords(values)
	m.TryRemoveOnUpdate(values)
	return m.Migrator.CreateTable(values...)
}

func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	for i := len(values) - 1; i >= 0; i-- {
		value := values[i]
		tx := m.DB.Session(&gorm.Session{})
		if m.HasTable(value) {
			if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
				return tx.Exec("DROP TABLE ? CASCADE CONSTRAINTS", clause.Table{Name: stmt.Table}).Error
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int64

	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw("SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = ?", stmt.Table).Row().Scan(&count)
	})

	return count > 0
}

func (m Migrator) RenameTable(oldName, newName interface{}) (err error) {
	resolveTable := func(name interface{}) (result string, err error) {
		if v, ok := name.(string); ok {
			result = v
		} else {
			stmt := &gorm.Statement{DB: m.DB}
			if err = stmt.Parse(name); err == nil {
				result = stmt.Table
			}
		}
		return
	}

	var oldTable, newTable string

	if oldTable, err = resolveTable(oldName); err != nil {
		return
	}

	if newTable, err = resolveTable(newName); err != nil {
		return
	}

	if !m.HasTable(oldTable) {
		return
	}

	return m.DB.Exec("RENAME TABLE ? TO ?",
		clause.Table{Name: oldTable},
		clause.Table{Name: newTable},
	).Error
}

func (m Migrator) AddColumn(value interface{}, field string) error {
	if !m.HasColumn(value, field) {
		return nil
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? ADD ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, m.DB.Migrator().FullDataTypeOf(field),
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) DropColumn(value interface{}, name string) error {
	if !m.HasColumn(value, name) {
		return nil
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(name); field != nil {
			name = field.DBName
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP ?",
			clause.Table{Name: stmt.Table},
			clause.Column{Name: name},
		).Error
	})
}

func (m Migrator) AlterColumn(value interface{}, field string) error {
	field = strings.ToUpper(field)
	if !m.HasColumn(value, field) {
		return nil
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? MODIFY ? ?",
				clause.Table{Name: stmt.Table},
				clause.Column{Name: field.DBName},
				m.FullDataTypeOf(field),
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw("SELECT COUNT(*) FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?", stmt.Table, field).Row().Scan(&count)
	}) == nil && count > 0
}

func (m Migrator) CreateConstraint(value interface{}, name string) error {
	m.TryRemoveOnUpdate(value)
	return m.Migrator.CreateConstraint(value, name)
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		for _, chk := range stmt.Schema.ParseCheckConstraints() {
			if chk.Name == name {
				return m.DB.Exec(
					"ALTER TABLE ? DROP CHECK ?",
					clause.Table{Name: stmt.Table}, clause.Column{Name: name},
				).Error
			}
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP CONSTRAINT ?",
			clause.Table{Name: stmt.Table}, clause.Column{Name: name},
		).Error
	})
}

func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM USER_CONSTRAINTS WHERE TABLE_NAME = ? AND CONSTRAINT_NAME = ?", stmt.Table, name,
		).Row().Scan(&count)
	}) == nil && count > 0
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}, clause.Table{Name: stmt.Table}).Error
	})
}

func (m Migrator) HasIndex(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Raw(
			"SELECT COUNT(*) FROM USER_INDEXES WHERE TABLE_NAME = ? AND INDEX_NAME = ?",
			m.Migrator.DB.NamingStrategy.TableName(stmt.Table),
			m.Migrator.DB.NamingStrategy.IndexName(stmt.Table, name),
		).Row().Scan(&count)
	})

	return count > 0
}

// https://docs.oracle.com/database/121/SPATL/alter-index-rename.htm
func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	panic("TODO")
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec(
			"ALTER INDEX ?.? RENAME TO ?", // wat
			clause.Table{Name: stmt.Table}, clause.Column{Name: oldName}, clause.Column{Name: newName},
		).Error
	})
}

func (m Migrator) TryRemoveOnUpdate(value interface{}) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		for _, rel := range stmt.Schema.Relationships.Relations {
			constraint := rel.ParseConstraint()
			if constraint != nil {
				rel.Field.TagSettings["CONSTRAINT"] = strings.ReplaceAll(rel.Field.TagSettings["CONSTRAINT"], fmt.Sprintf("ON UPDATE %s", constraint.OnUpdate), "")
			}
		}
		return nil
	})
}

func (m Migrator) TryQuotifyReservedWords(values []interface{}) error {
	return m.RunWithValue(values, func(stmt *gorm.Statement) error {
		for idx, v := range stmt.Schema.DBNames {
			if IsReservedWord(v) {
				stmt.Schema.DBNames[idx] = fmt.Sprintf(`"%s"`, v)
			}
		}

		for _, v := range stmt.Schema.Fields {
			if IsReservedWord(v.DBName) {
				v.DBName = fmt.Sprintf(`"%s"`, v.DBName)
			}
		}
		return nil
	})
}
func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (string, string) {
	if tables := strings.Split(table, `.`); len(tables) == 2 {
		return tables[0], tables[1]
	}
	m.DB = m.DB.Table(table)
	return m.CurrentDatabase(), strings.ToUpper(table)
}

// ColumnTypes column types return columnTypes,error
func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	columnTypes := make([]gorm.ColumnType, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var (
			_, table      = m.CurrentSchema(stmt, stmt.Table)
			columnTypeSQL = "SELECT COLUMN_NAME, DATA_DEFAULT, DECODE(NULLABLE,'Y',1,0), DATA_TYPE, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE "
			rows, err     = m.DB.Session(&gorm.Session{}).Table(table).Limit(1).Rows()
		)

		if err != nil {
			return err
		}

		rawColumnTypes, err := rows.ColumnTypes()

		if err != nil {
			return err
		}

		if err := rows.Close(); err != nil {
			return err
		}

		// if !m.DisableDatetimePrecision {
		// 	columnTypeSQL += ", datetime_precision "
		// }
		columnTypeSQL += "FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? ORDER BY COLUMN_ID"

		columns, rowErr := m.DB.Debug().Table(table).Raw(columnTypeSQL, table).Rows()
		if rowErr != nil {
			return rowErr
		}

		defer columns.Close()

		for columns.Next() {
			var (
				column            migrator.ColumnType
				datetimePrecision sql.NullInt64
				// extraValue        sql.NullString
				// columnKey         sql.NullString
				values = []interface{}{
					&column.NameValue,
					&column.DefaultValueValue,
					&column.NullableValue,
					&column.DataTypeValue,
					&column.LengthValue,
					// &column.ColumnTypeValue,
					// &columnKey,
					// &extraValue,
					// &column.CommentValue,
					&column.DecimalSizeValue,
					&column.ScaleValue,
				}
			)

			// if !m.DisableDatetimePrecision {
			// 	values = append(values, &datetimePrecision)
			// }

			if scanErr := columns.Scan(values...); scanErr != nil {
				return scanErr
			}

			column.PrimaryKeyValue = sql.NullBool{Bool: false, Valid: true}
			column.UniqueValue = sql.NullBool{Bool: false, Valid: true}
			// switch columnKey.String {
			// case "PRI":
			// 	column.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
			// case "UNI":
			// 	column.UniqueValue = sql.NullBool{Bool: true, Valid: true}
			// }

			// if strings.Contains(extraValue.String, "auto_increment") {
			// 	column.AutoIncrementValue = sql.NullBool{Bool: true, Valid: true}
			// }

			// only trim paired single-quotes
			s := column.DefaultValueValue.String
			for (len(s) >= 3 && s[0] == '\'' && s[len(s)-1] == '\'' && s[len(s)-2] != '\\') ||
				(len(s) == 2 && s == "''") {
				s = s[1 : len(s)-1]
			}
			column.DefaultValueValue.String = s
			// if m.Dialector.DontSupportNullAsDefaultValue {
			// 	// rewrite mariadb default value like other version
			// 	if column.DefaultValueValue.Valid && column.DefaultValueValue.String == "NULL" {
			// 		column.DefaultValueValue.Valid = false
			// 		column.DefaultValueValue.String = ""
			// 	}
			// }

			if datetimePrecision.Valid {
				column.DecimalSizeValue = datetimePrecision
			}

			for _, c := range rawColumnTypes {
				if c.Name() == column.NameValue.String {
					column.SQLColumnType = c
					break
				}
			}
			column.NameValue.String = strings.ToLower(column.NameValue.String)

			columnTypes = append(columnTypes, column)
		}

		return nil
	})

	return columnTypes, err
}
