package basic;
import static org.junit.Assert.*;

import org.junit.Test;

import basic.Basic;

public class BasicTest {

	@Test
	public void test() {
		Basic basic = new Basic();
		assertTrue(basic.isBasic());
	}

}
